#include "RedisClient.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <sstream>
#include <cstring>
#include <algorithm>
#include <hiredis/hiredis.h>

// ============================================================
// 内部工具: 简易 RESP 协议编码
// ============================================================
static std::string respEncodeArgv(int argc, const char** argv, const size_t* argvLen) {
    std::string cmd;
    cmd.reserve(256);
    cmd += "*" + std::to_string(argc) + "\r\n";
    for (int i = 0; i < argc; ++i) {
        cmd += "$" + std::to_string(argvLen[i]) + "\r\n";
        cmd.append(argv[i], argvLen[i]);
        cmd += "\r\n";
    }
    return cmd;
}

// 单命令 RESP 编码
// 更安全的封装：直接传参数列表，不自行分割字符串
static std::string respEncode(const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> argvLen;
    argv.reserve(args.size());
    argvLen.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
        argvLen.push_back(arg.size());
    }
    return respEncodeArgv(static_cast<int>(args.size()), argv.data(), argvLen.data());
}

// ============================================================
// RedisClient 构造与析构
// ============================================================
RedisClient::RedisClient(EventLoop* loop, const RedisConfig& cfg)
    : loop_(loop)
    , config_(cfg)
    , connected_(false) {
}

RedisClient::~RedisClient() {
    disconnect();
}

// ============================================================
// connect — 连接 Redis (含重连逻辑)
// ============================================================
bool RedisClient::connect() {
    if (connected_) return true;

    struct timeval timeout = {config_.connTimeoutMs / 1000,
                             (config_.connTimeoutMs % 1000) * 1000};

    conn_.ctx = redisConnectWithTimeout(config_.host.c_str(),
                                        config_.port, timeout);
    if (!conn_.ctx || (reinterpret_cast<redisContext*>(conn_.ctx))->err) {
        if (conn_.ctx) {
            LOG_ERROR("RedisClient: connection failed: %s",
                      reinterpret_cast<redisContext*>(conn_.ctx)->errstr);
        } else {
            LOG_ERROR("RedisClient: can't allocate redis context");
        }
        disconnect();
        return false;
    }

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    redisSetTimeout(ctx, timeout);

    // 密码认证
    if (!config_.password.empty()) {
        auto* reply = reinterpret_cast<redisReply*>(
            redisCommand(ctx, "AUTH %s", config_.password.c_str()));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR("RedisClient: AUTH failed");
            if (reply) freeReplyObject(reply);
            disconnect();
            return false;
        }
        freeReplyObject(reply);
    }

    // 选择数据库
    if (config_.db != 0) {
        auto* reply = reinterpret_cast<redisReply*>(
            redisCommand(ctx, "SELECT %d", config_.db));
        freeReplyObject(reply);
    }

    // 创建订阅连接 (用于 Pub/Sub)
    subConn_.ctx = redisConnectWithTimeout(config_.host.c_str(),
                                           config_.port, timeout);
    if (!subConn_.ctx || (reinterpret_cast<redisContext*>(subConn_.ctx))->err) {
        LOG_ERROR("RedisClient: subscription connection failed");
        // 订阅连接失败不影响主连接
    }

    connected_ = true;
    LOG_INFO("RedisClient: connected to %s:%d", config_.host.c_str(), config_.port);
    return true;
}

void RedisClient::disconnect() {
    connected_ = false;
    subActive_ = false;

    if (conn_.ctx) {
        redisFree(reinterpret_cast<redisContext*>(conn_.ctx));
        conn_.ctx = nullptr;
    }
    if (subConn_.ctx) {
        redisFree(reinterpret_cast<redisContext*>(subConn_.ctx));
        subConn_.ctx = nullptr;
    }
}

bool RedisClient::reconnect() {
    disconnect();
    return connect();
}

// ============================================================
// Redis Streams 消费者组 — 分布式核心
//
// XREADGROUP 工作流程:
//  1. 创建消费者组: XGROUP CREATE stream group $ MKSTREAM
//     (幂等操作, 重复创建返回 BUSYGROUP 错误, 忽略)
//  2. 消费消息:   XREADGROUP GROUP group consumer COUNT N BLOCK 0 STREAMS stream >
//     ">" 表示只消费从未投递过的新消息
//  3. 确认消费:   XACK stream group message-id
//  4. 认领超时:   XPENDING stream group - + N → 获取未确认消息
//                 XCLAIM stream group consumer min-idle-time message-ids
//  ============================================================
bool RedisClient::initConsumerGroup() {
    if (!connected_) return false;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);

    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XGROUP CREATE %s %s $ MKSTREAM",
        config_.streamKey.c_str(), config_.consumerGroup.c_str()));

    if (!reply) {
        LOG_ERROR("RedisClient: XGROUP CREATE failed — connection issue");
        return false;
    }

    bool ok = true;
    if (reply->type == REDIS_REPLY_ERROR) {
        // BUSYGROUP 表示消费者组已存在, 不是错误
        if (strstr(reply->str, "BUSYGROUP")) {
            LOG_INFO("RedisClient: consumer group [%s] already exists, reusing",
                     config_.consumerGroup.c_str());
        } else {
            LOG_ERROR("RedisClient: XGROUP CREATE error: %s", reply->str);
            ok = false;
        }
    }
    freeReplyObject(reply);

    LOG_INFO("RedisClient: consumer group [%s] on stream [%s] ready, "
             "consumer [%s]",
             config_.consumerGroup.c_str(), config_.streamKey.c_str(),
             config_.consumerName.c_str());
    return ok;
}

std::vector<Command> RedisClient::consumeCommands(int batchSize, int blockMs) {
    if (!connected_) return {};

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);

    // XREADGROUP GROUP <group> <consumer> COUNT <count> BLOCK <ms> STREAMS <key> >
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XREADGROUP GROUP %s %s COUNT %d BLOCK %d STREAMS %s >",
        config_.consumerGroup.c_str(), config_.consumerName.c_str(),
        batchSize, blockMs, config_.streamKey.c_str()));

    if (!reply) {
        // 连接断开
        LOG_WARN("RedisClient: XREADGROUP failed, attempting reconnect");
        reconnect();
        return {};
    }

    std::vector<Command> commands;

    if (reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
        // reply[0] 是 stream key
        // reply[0]->element[1] 是消息数组
        if (reply->element[0]->elements > 1) {
            redisReply* msgs = reply->element[0]->element[1];
            for (size_t i = 0; i < msgs->elements; ++i) {
                redisReply* msg = msgs->element[i];
                // msg->element[0] = message ID
                // msg->element[1] = field-value pairs array

                std::string streamId(msg->element[0]->str, msg->element[0]->len);
                redisReply* fields = msg->element[1];

                std::unordered_map<std::string, std::string> fieldMap;
                for (size_t j = 0; j + 1 < fields->elements; j += 2) {
                    std::string key(fields->element[j]->str, fields->element[j]->len);
                    std::string val(fields->element[j+1]->str, fields->element[j+1]->len);
                    fieldMap[key] = val;
                }

                Command cmd;
                cmd.streamId = streamId;
                auto idIt = fieldMap.find("id");
                if (idIt != fieldMap.end()) {
                    cmd.id = std::stoll(idIt->second);
                }
                auto devIt = fieldMap.find("device_id");
                if (devIt != fieldMap.end()) {
                    cmd.deviceId = devIt->second;
                }
                auto topicIt = fieldMap.find("topic");
                if (topicIt != fieldMap.end()) {
                    cmd.topic = topicIt->second;
                }
                auto payloadIt = fieldMap.find("payload");
                if (payloadIt != fieldMap.end()) {
                    cmd.payload = payloadIt->second;
                }
                auto timeoutIt = fieldMap.find("timeout");
                if (timeoutIt != fieldMap.end()) {
                    cmd.timeout = std::stoi(timeoutIt->second);
                }
                cmd.createdAt = Timestamp::now().microSecondsSinceEpoch();

                commands.push_back(cmd);
            }
        }
    }

    freeReplyObject(reply);

    if (!commands.empty()) {
        LOG_DEBUG("RedisClient: consumed %zu commands from stream [%s] "
                  "(consumer: %s)",
                  commands.size(), config_.streamKey.c_str(),
                  config_.consumerName.c_str());
    }
    return commands;
}

bool RedisClient::ackCommand(const std::string& streamId) {
    if (!connected_) return false;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XACK %s %s %s",
        config_.streamKey.c_str(), config_.consumerGroup.c_str(),
        streamId.c_str()));
    bool ok = (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer > 0);
    if (reply) freeReplyObject(reply);
    return ok;
}

int64_t RedisClient::pendingCount() {
    if (!connected_) return 0;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XPENDING %s %s - + 1000",
        config_.streamKey.c_str(), config_.consumerGroup.c_str()));
    int64_t count = 0;
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        count = reply->elements;
    }
    if (reply) freeReplyObject(reply);
    return count;
}

// ============================================================
// 指令发布 (API 侧)
// ============================================================
std::string RedisClient::publishCommand(const Command& cmd) {
    if (!connected_) return "";
    int64_t ts = Timestamp::now().microSecondsSinceEpoch();

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XADD %s * id %lld device_id %s topic %s payload %s timeout %d created_at %lld",
        config_.streamKey.c_str(),
        (long long)(cmd.id ? cmd.id : ts),
        cmd.deviceId.c_str(),
        cmd.topic.empty() ? (std::string("/cmd/") + cmd.deviceId + "/down").c_str() : cmd.topic.c_str(),
        cmd.payload.c_str(),
        cmd.timeout ? cmd.timeout : 30,
        (long long)ts));

    std::string streamId;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        streamId.assign(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);

    LOG_INFO("RedisClient: published command [%lld] to stream [%s], streamId=%s",
             (long long)(cmd.id ? cmd.id : ts), config_.streamKey.c_str(),
             streamId.c_str());
    return streamId;
}

// ============================================================
// 指令状态管理 (Sorted Set + Hash)
//
// Key 设计:
//   iot:cmd:{id} — Hash: 指令详情 (status, device_id, response, etc.)
//   iot:cmd:pending — Sorted Set: score=timeout_timestamp, member=command_id
// ============================================================
bool RedisClient::markCommandSent(int64_t commandId) {
    if (!connected_) return true;

    int64_t now = Timestamp::now().microSecondsSinceEpoch() / 1000000;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    // 更新状态 + 加入超时扫描集合
    redisAppendCommand(ctx,
        "HSET iot:cmd:%lld status sent sent_at %lld", (long long)commandId, (long long)now);
    // 超时 = 当前时间 + 30秒
    redisAppendCommand(ctx,
        "ZADD iot:cmd:pending %lld %lld", (long long)(now + 30), (long long)commandId);
    redisReply* reply = nullptr;
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    return true;
}

bool RedisClient::markCommandFailed(int64_t commandId, const std::string& reason) {
    if (!connected_) return true;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HSET iot:cmd:%lld status failed reason %s",
        (long long)commandId, reason.c_str()));
    if (reply) freeReplyObject(reply);
    // 从超时扫描集合中移除
    reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "ZREM iot:cmd:pending %lld", (long long)commandId));
    if (reply) freeReplyObject(reply);
    return true;
}

bool RedisClient::markCommandCompleted(int64_t commandId, const std::string& response) {
    if (!connected_) return true;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    redisAppendCommand(ctx,
        "HSET iot:cmd:%lld status completed response %s",
        (long long)commandId, response.c_str());
    redisAppendCommand(ctx,
        "ZREM iot:cmd:pending %lld", (long long)commandId);
    redisReply* reply = nullptr;
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    // 设置过期时间 (1小时后自动清理)
    reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "EXPIRE iot:cmd:%lld 3600", (long long)commandId));
    if (reply) freeReplyObject(reply);
    return true;
}

std::string RedisClient::getCommandStatus(int64_t commandId) {
    if (!connected_) return "unknown";

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGET iot:cmd:%lld status", (long long)commandId));
    std::string status;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        status.assign(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);
    return status.empty() ? "unknown" : status;
}

std::vector<int64_t> RedisClient::scanTimeoutCommands(int64_t timeoutUs) {
    if (!connected_) return {};

    int64_t now = Timestamp::now().microSecondsSinceEpoch() / 1000000;
    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "ZRANGEBYSCORE iot:cmd:pending -inf %lld",
        (long long)now));
    std::vector<int64_t> ids;
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; ++i) {
            ids.push_back(std::stoll(
                std::string(reply->element[i]->str, reply->element[i]->len)));
        }
    }
    if (reply) freeReplyObject(reply);
    return ids;
}

// ============================================================
// 分布式设备注册表 — 核心分布式特性
//
// 所有网关节点共享同一份设备注册表 (存储在 Redis Hash)
// 设备连接哪个节点, 就注册到哪个节点
// 跨节点指令下发时, 先查注册表找到目标节点, 再通过 Pub/Sub 转发
// ============================================================
bool RedisClient::registerDevice(const std::string& clientId,
                                const std::string& gatewayNodeId,
                                int keepAlive) {
    if (!connected_) return false;

    int64_t now = Timestamp::now().microSecondsSinceEpoch();

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    char json[512];
    snprintf(json, sizeof(json),
        R"({"gateway":"%s","heartbeat":%lld,"keepalive":%d})",
        gatewayNodeId.c_str(), (long long)(now / 1000000), keepAlive);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HSET iot:devices %s %s", clientId.c_str(), json));
    if (reply) freeReplyObject(reply);

    LOG_DEBUG("RedisClient: registered device [%s] on node [%s] in distributed registry",
             clientId.c_str(), gatewayNodeId.c_str());
    return true;
}

bool RedisClient::unregisterDevice(const std::string& clientId) {
    if (!connected_) return false;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HDEL iot:devices %s", clientId.c_str()));
    if (reply) freeReplyObject(reply);

    // 同时清理遥测缓存
    reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "DEL iot:telemetry:%s", clientId.c_str()));
    if (reply) freeReplyObject(reply);
    return true;
}

std::string RedisClient::getDeviceGateway(const std::string& clientId) {
    if (!connected_) return "";

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGET iot:devices %s", clientId.c_str()));
    std::string result;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        result.assign(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);
    return result;
}

std::vector<std::pair<std::string, std::string>> RedisClient::getAllDevices() {
    std::vector<std::pair<std::string, std::string>> result;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGETALL iot:devices"));
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i + 1 < reply->elements; i += 2) {
            std::string clientId(reply->element[i]->str, reply->element[i]->len);
            std::string json(reply->element[i+1]->str, reply->element[i+1]->len);
            result.emplace_back(clientId, json);
        }
    }
    if (reply) freeReplyObject(reply);
    return result;
}

size_t RedisClient::deviceCount() {
    if (!connected_) return 0;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HLEN iot:devices"));
    size_t count = 0;
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        count = static_cast<size_t>(reply->integer);
    }
    if (reply) freeReplyObject(reply);
    return count;
}

bool RedisClient::updateDeviceHeartbeat(const std::string& clientId) {
    if (!connected_) return false;

    int64_t now = Timestamp::now().microSecondsSinceEpoch() / 1000000;
    // 读取现有数据, 更新心跳时间
    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGET iot:devices %s", clientId.c_str()));
    if (reply && reply->type == REDIS_REPLY_STRING) {
        std::string json(reply->str, reply->len);
        // 简易替换: 修改 heartbeat 字段
        auto pos = json.find("\"heartbeat\":");
        if (pos != std::string::npos) {
            auto numStart = pos + 12;
            auto numEnd = json.find_first_of(",}", numStart);
            if (numEnd != std::string::npos) {
                json.replace(numStart, numEnd - numStart, std::to_string(now));
                redisReply* r2 = reinterpret_cast<redisReply*>(redisCommand(ctx,
                    "HSET iot:devices %s %s", clientId.c_str(), json.c_str()));
                if (r2) freeReplyObject(r2);
            }
        }
    }
    if (reply) freeReplyObject(reply);
    return true;
}

// ============================================================
// 网关节点管理 — 注册 + 心跳
//
// 每个网关节点在 Redis 中注册自己:
//   HSET iot:nodes {nodeId} {...}
//   心跳: EXPIRE iot:nodes:{nodeId} 15 (TTL 15秒)
//   定期刷新 TTL, 超时未刷新则节点视作离线
// ============================================================
bool RedisClient::registerNode(const std::string& nodeId,
                              const std::string& addr,
                              uint16_t mqttPort, uint16_t apiPort) {
    if (!connected_) return false;

    int64_t now = Timestamp::now().microSecondsSinceEpoch() / 1000000;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    char info[256];
    snprintf(info, sizeof(info),
        R"({"addr":"%s","mqtt_port":%u,"api_port":%u,"started_at":%lld})",
        addr.c_str(), mqttPort, apiPort, (long long)now);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HSET iot:nodes %s %s", nodeId.c_str(), info));
    if (reply) freeReplyObject(reply);

    LOG_INFO("RedisClient: node [%s] registered (mqtt=%u, api=%u)",
             nodeId.c_str(), mqttPort, apiPort);
    return true;
}

bool RedisClient::sendNodeHeartbeat(const std::string& nodeId) {
    if (!connected_) return false;

    // 使用独立的 Key + TTL 做心跳
    // EXPIRE 刷新 TTL, 15秒无心跳则 key 被删除
    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);

    // Pipeline: HSET + EXPIRE
    redisAppendCommand(ctx, "HSET iot:nodes:heartbeat %s 1", nodeId.c_str());
    redisAppendCommand(ctx, "EXPIRE iot:nodes:heartbeat 15");

    redisReply* reply = nullptr;
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    redisGetReply(ctx, (void**)&reply); freeReplyObject(reply);
    return true;
}

std::vector<std::string> RedisClient::getOnlineNodes() {
    std::vector<std::string> result;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    // 获取有心跳的所有节点
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGETALL iot:nodes:heartbeat"));
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; i += 2) {
            std::string nodeId(reply->element[i]->str, reply->element[i]->len);
            result.push_back(nodeId);
        }
    }
    if (reply) freeReplyObject(reply);
    return result;
}

std::string RedisClient::getNodeInfo(const std::string& nodeId) {
    if (!connected_) return "";

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HGET iot:nodes %s", nodeId.c_str()));
    std::string result;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        result.assign(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);
    return result;
}

// ============================================================
// 跨节点消息路由 — Pub/Sub
//
// 当指令目标设备不在本节点时, 通过 Pub/Sub 转发:
//   PUBLISH iot:route:{target_node_id} {json}
//
// 每个节点订阅自己的 channel:
//   SUBSCRIBE iot:route:{my_node_id}
//
// 设计要点:
//  - Pub/Sub 是即发即忘的, 消息不会持久化
//  - 如果目标节点恰好挂掉, 消息丢失
//  - 可通过 Streams 做降级: 定期扫描未 ACK 指令, 超时重投
// ============================================================
bool RedisClient::subscribeRouteChannel(const std::string& nodeId,
                                       CrossNodeCmdCallback cb) {
    routeCallback_ = std::move(cb);

    if (!subConn_.ctx) {
        LOG_WARN("RedisClient: subscription connection not available");
        return false;
    }

    redisContext* subCtx = reinterpret_cast<redisContext*>(subConn_.ctx);

    // SUBSCRIBE iot:route:{nodeId}
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(subCtx,
        "SUBSCRIBE iot:route:%s", nodeId.c_str()));
    if (reply) freeReplyObject(reply);

    subActive_ = true;
    LOG_INFO("RedisClient: subscribed to route channel [iot:route:%s]",
             nodeId.c_str());

    // 注意: 在真实实现中需要一个独立的 EventLoop 线程来持续读取
    // 订阅连接的回复。这里简化, 在 consumeCommands 中轮询检查。
    return true;
}

bool RedisClient::publishToNode(const std::string& targetNodeId,
                               const Command& cmd) {
    if (!connected_) return false;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);

    // 序列化指令为 JSON
    char json[1024];
    snprintf(json, sizeof(json),
        R"({"id":%lld,"device_id":"%s","topic":"%s","payload":%s,"timeout":%d})",
        (long long)cmd.id,
        cmd.deviceId.c_str(),
        cmd.topic.c_str(),
        cmd.payload.c_str(),
        cmd.timeout);

    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "PUBLISH iot:route:%s %s", targetNodeId.c_str(), json));
    int64_t receivers = 0;
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        receivers = reply->integer;
    }
    if (reply) freeReplyObject(reply);

    LOG_DEBUG("RedisClient: forwarded command [%lld] to node [%s] (received by %lld subscribers)",
             (long long)cmd.id, targetNodeId.c_str(), (long long)receivers);
    return receivers > 0;
}

// ============================================================
// 设备遥测数据缓存
// ============================================================
bool RedisClient::cacheTelemetry(const std::string& clientId,
                                const std::string& topic,
                                const std::string& payload) {
    if (!connected_) return false;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "HSET iot:telemetry:%s %s %s",
        clientId.c_str(), topic.c_str(), payload.c_str()));
    if (reply) freeReplyObject(reply);

    // TTL: 5分钟过期
    reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "EXPIRE iot:telemetry:%s 300", clientId.c_str()));
    if (reply) freeReplyObject(reply);
    return true;
}

// ============================================================
// 统计信息
// ============================================================
int64_t RedisClient::streamLength() {
    if (!connected_) return 0;

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XLEN %s", config_.streamKey.c_str()));
    int64_t len = 0;
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        len = reply->integer;
    }
    if (reply) freeReplyObject(reply);
    return len;
}

std::string RedisClient::consumerGroupInfo() {
    if (!connected_) return "{}";

    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx,
        "XINFO GROUPS %s", config_.streamKey.c_str()));
    std::string result;
    if (reply) {
        if (reply->type == REDIS_REPLY_ARRAY) {
            result = std::to_string(reply->elements) + " consumer groups";
        }
        freeReplyObject(reply);
    }
    return result;
}

// ============================================================
// 底层命令执行 (hiredis)
// ============================================================
std::string RedisClient::sendCommand(const std::string& cmd) {
    if (!conn_.ctx) return "";
    redisContext* ctx = reinterpret_cast<redisContext*>(conn_.ctx);
    auto* reply = reinterpret_cast<redisReply*>(redisCommand(ctx, cmd.c_str()));
    std::string result;
    if (reply) {
        if (reply->type == REDIS_REPLY_STRING) {
            result.assign(reply->str, reply->len);
        } else if (reply->type == REDIS_REPLY_INTEGER) {
            result = std::to_string(reply->integer);
        } else if (reply->type == REDIS_REPLY_ERROR) {
            result = std::string("ERR:") + reply->str;
        }
        freeReplyObject(reply);
    }
    return result;
}
