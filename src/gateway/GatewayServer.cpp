#include "GatewayServer.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <unordered_map>

// TcpConnection* → clientId 映射 (在 mainLoop 访问, 无需锁)
static std::unordered_map<void*, std::string> g_connClientMap;

GatewayServer::GatewayServer(EventLoop* loop, uint16_t mqttPort,
                             const RedisConfig& redisCfg,
                             const TsdbConfig& tsdbCfg)
    : loop_(loop)
    , mqttPort_(mqttPort)
    , running_(false)
    , redisClient_(loop, redisCfg)
    , tsdbClient_(loop, tsdbCfg)
    , clusterMgr_(loop, &redisClient_) {

    auto listenAddr = InetAddress(mqttPort);
    tcpServer_ = std::make_unique<TcpServer>(loop, listenAddr, "MQTT-Gateway");
}

GatewayServer::~GatewayServer() {
    stop();
}

void GatewayServer::setThreadNum(int num) {
    tcpServer_->setThreadNum(num);
}

void GatewayServer::setNodeId(const std::string& nodeId) {
    nodeId_ = nodeId;
    redisClient_.setConsumerName(nodeId);  // 消费者名 = 节点名
    deviceMgr_.setNodeId(nodeId);
}


// ============================================================
// 启动 — 初始化分布式组件
//
// 启动顺序:
//  1. 连接 Redis (分布式注册表 + Streams)
//  2. 初始化消费者组 (XGROUP CREATE)
//  3. 初始化集群管理器 (节点注册 + 路由通道订阅)
//  4. 加入集群
//  5. 注册 MQTT 回调
//  6. 启动 TCP 服务器 (accept 设备连接)
//  7. 启动心跳 + 故障检测
//  8. 启动指令消费者
// ============================================================
void GatewayServer::start() {
    running_ = true;

    // 1. 连接 Redis
    LOG_INFO("GatewayServer: connecting to Redis %s:%d...",
             redisClient_.getConfig().host.c_str(), redisClient_.getConfig().port);
    if (!redisClient_.connect()) {
        LOG_WARN("GatewayServer: Redis connection failed, "
                 "running in single-node mode");
    }

    // 2. 初始化消费者组 (幂等操作)
    if (redisClient_.isConnected()) {
        redisClient_.initConsumerGroup();
    }

    // 3. 初始化集群管理器
    clusterMgr_.init(nodeId_, "0.0.0.0", mqttPort_, 8080);

    // 4. 加入到集群
    if (redisClient_.isConnected()) {
        clusterMgr_.join();
        // 订阅跨节点路由通道
        clusterMgr_.subscribeRouteChannel(
            [this](const Command& cmd) { processCrossNodeCommand(cmd); });
    }

    // 5. 设置 DeviceManager 的 Redis 后端
    deviceMgr_.setRedisClient(&redisClient_);
    deviceMgr_.setNodeId(nodeId_);

    // 6. 注册 MQTT 回调
    codec_.setConnectCallback(
        [this](const MqttConnectPayload& p) { handleConnect(currentConn_, p); });
    codec_.setPublishCallback(
        [this](const std::string& topic, const std::string& payload,
               MqttQoS qos, bool retain, uint16_t pktId) {
            handlePublish(currentConn_, topic, payload, qos, retain, pktId);
        });
    codec_.setSubscribeCallback(
        [this](uint16_t pktId, const std::vector<MqttTopicFilter>& filters) {
            handleSubscribe(currentConn_, pktId, filters);
        });
    codec_.setPingReqCallback(
        [this]() { handlePingReq(currentConn_); });
    codec_.setDisconnectCallback(
        [this]() { onDisconnect(currentConn_); });

    tcpServer_->setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { onConnection(conn); });
    tcpServer_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            onMessage(conn, buf, ts);
        });

    // 7. 启动 TCP 服务器
    tcpServer_->start();

    // 8. 启动后台任务
    loop_->runEvery(5.0, [this] { startHeartbeatCheck(); });
    loop_->runEvery(0.5, [this] { processCommand(); });

    // 集群心跳 (每5秒)
    if (redisClient_.isConnected()) {
        clusterMgr_.startHeartbeat(5.0);
        clusterMgr_.startFailureDetection(10.0);
    }

    LOG_INFO("GatewayServer [%s] started on port %u (distributed mode)",
             nodeId_.c_str(), mqttPort_);
}

void GatewayServer::stop() {
    running_ = false;
    clusterMgr_.leave();
    redisClient_.disconnect();
}

// ============================================================
// 连接回调
// ============================================================
void GatewayServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        LOG_INFO("[%s] New connection from %s",
                 nodeId_.c_str(), conn->peerAddr().toIpPort().c_str());
    } else {
        auto it = g_connClientMap.find(conn.get());
        if (it != g_connClientMap.end()) {
            deviceMgr_.deviceOffline(it->second);
            g_connClientMap.erase(it);
        }
        LOG_INFO("[%s] Connection %s closed",
                 nodeId_.c_str(), conn->name().c_str());
    }
}

// ============================================================
// 消息回调 — MQTT 报文入口 (TCP粘包/半包处理)
// ============================================================
void GatewayServer::onMessage(const TcpConnectionPtr& conn,
                              Buffer* buf, Timestamp ts) {
    currentConn_ = conn;
    while (buf->readableBytes() > 0) {
        auto result = codec_.parse(buf);
        if (result == MqttCodec::ParseResult::INCOMPLETE) {
            break;
        }
        if (result == MqttCodec::ParseResult::ERROR) {
            LOG_ERROR("[%s] MQTT parse error from %s, closing",
                      nodeId_.c_str(), conn->name().c_str());
            conn->forceClose();
            break;
        }
    }
}

// ============================================================
// CONNECT 处理 — 设备认证 + 分布式注册
// ============================================================
void GatewayServer::handleConnect(const TcpConnectionPtr& conn,
                                  const MqttConnectPayload& payload) {
    // 注册到本地 DeviceManager (自动同步到 Redis)
    deviceMgr_.deviceOnline(payload.clientId, conn,
                           payload.keepAlive, payload.username);
    g_connClientMap[conn.get()] = payload.clientId;

    // 回复 CONNACK
    Buffer ack;
    MqttCodec::encodeConnAck(&ack, MqttConnAckCode::ACCEPTED);
    conn->send(&ack);

    LOG_INFO("[%s] Device [%s] authenticated, keepAlive=%d",
             nodeId_.c_str(), payload.clientId.c_str(), payload.keepAlive);
}

// ============================================================
// PUBLISH 处理 — 数据上报 & 消息转发
// ============================================================
void GatewayServer::handlePublish(const TcpConnectionPtr& conn,
                                  const std::string& topic,
                                  const std::string& payload,
                                  MqttQoS qos, bool retain,
                                  uint16_t packetId) {
    auto it = g_connClientMap.find(conn.get());
    std::string clientId = (it != g_connClientMap.end()) ? it->second : "unknown";

    LOG_DEBUG("[%s] PUBLISH from [%s] topic=%s payload=%zu bytes qos=%d",
              nodeId_.c_str(), clientId.c_str(), topic.c_str(),
              payload.size(), static_cast<int>(qos));

    // 1. 写入时序数据库 (异步批量, 不阻塞)
    tsdbClient_.writeDeviceData(clientId, topic, payload);

    // 2. 缓存最新遥测数据到 Redis (供管理平台查询)
    if (redisClient_.isConnected()) {
        redisClient_.cacheTelemetry(clientId, topic, payload);
    }

    // 3. 转发给本节点订阅者 (1:N 消息路由)
    auto subscribers = deviceMgr_.getSubscribers(topic);
    for (auto& subConn : subscribers) {
        if (subConn.get() != conn.get()) {
            Buffer fwd;
            MqttCodec::encodePublish(&fwd, topic, payload,
                                     MqttQoS::AT_MOST_ONCE, 0);
            subConn->send(&fwd);
        }
    }

    // 4. QoS 1 → 回复 PUBACK
    if (qos == MqttQoS::AT_LEAST_ONCE) {
        Buffer ack;
        MqttCodec::encodePubAck(&ack, packetId);
        conn->send(&ack);
    }
}

// ============================================================
// SUBSCRIBE 处理
// ============================================================
void GatewayServer::handleSubscribe(const TcpConnectionPtr& conn,
                                   uint16_t packetId,
                                   const std::vector<MqttTopicFilter>& filters) {
    auto it = g_connClientMap.find(conn.get());
    if (it == g_connClientMap.end()) return;

    std::vector<uint8_t> returnCodes;
    for (const auto& filter : filters) {
        deviceMgr_.subscribe(it->second, filter.topic, filter.qos);
        returnCodes.push_back(filter.qos);
    }

    Buffer ack;
    MqttCodec::encodeSubAck(&ack, packetId, returnCodes);
    conn->send(&ack);
}

// ============================================================
// PINGREQ 处理 — 心跳
// ============================================================
void GatewayServer::handlePingReq(const TcpConnectionPtr& conn) {
    auto it = g_connClientMap.find(conn.get());
    if (it != g_connClientMap.end()) {
        deviceMgr_.updateHeartbeat(it->second);
    }

    Buffer resp;
    MqttCodec::encodePingResp(&resp);
    conn->send(&resp);
}

// ============================================================
// DISCONNECT 处理
// ============================================================
void GatewayServer::onDisconnect(const TcpConnectionPtr& conn) {
    auto it = g_connClientMap.find(conn.get());
    if (it != g_connClientMap.end()) {
        deviceMgr_.deviceOffline(it->second);
        g_connClientMap.erase(it);
    }
}

// ============================================================
// 心跳超时检查
// ============================================================
void GatewayServer::startHeartbeatCheck() {
    auto timeoutDevices = deviceMgr_.checkHeartbeatTimeout();
    for (const auto& clientId : timeoutDevices) {
        LOG_INFO("[%s] Device heartbeat timeout: [%s]",
                 nodeId_.c_str(), clientId.c_str());
    }
}

// ============================================================
// 指令消费 — 从 Redis Streams 消费者组拉取指令
//
// 分布式特性:
//  1. 多网关节点通过 XREADGROUP 竞争消费
//  2. Redis 保证每条消息只投递给组内一个消费者
//  3. 消费后 XACK 确认, 防止重复投递
//  4. 未 ACK 消息可通过 XCLAIM 被其他节点认领
//
// 指令路由:
//  查本地设备表:
//    - 设备在本节点 → 直接 MQTT PUBLISH 下发
//    - 设备在其他节点 → 通过 Pub/Sub 转发到目标节点
//    - 设备不在线 → 标记失败
// ============================================================
void GatewayServer::processCommand() {
    if (!running_) return;

    auto commands = redisClient_.consumeCommands(10, 0);  // 非阻塞
    if (commands.empty()) return;

    LOG_DEBUG("[%s] Consumed %zu commands from Redis Streams",
             nodeId_.c_str(), commands.size());

    for (const auto& cmd : commands) {
        cmdConsumed_++;

        // 先查找设备是否在本节点
        auto* device = deviceMgr_.findDevice(cmd.deviceId);

        if (device && device->isOnline()) {
            // 设备在本节点 → 直接下发
            sendCommandToDevice(cmd);
        } else {
            // 设备不在本节点 → 查分布式注册表
            std::string targetNode = deviceMgr_.findDeviceNode(cmd.deviceId);

            if (targetNode == nodeId_) {
                // 注册表显示在本节点, 但实际连接已断
                redisClient_.markCommandFailed(cmd.id, "device disconnected");
                LOG_WARN("[%s] Device [%s] registered here but not connected",
                         nodeId_.c_str(), cmd.deviceId.c_str());
            } else if (!targetNode.empty()) {
                // 设备在其他节点 → 跨节点转发
                bool forwarded = clusterMgr_.forwardCommand(cmd.deviceId, cmd);
                if (forwarded) {
                    cmdForwarded_++;
                    redisClient_.ackCommand(cmd.streamId);
                } else {
                    redisClient_.markCommandFailed(cmd.id, "forward failed");
                }
            } else {
                // 设备完全不在线
                redisClient_.markCommandFailed(cmd.id, "device offline");
                LOG_WARN("[%s] Command [%lld] failed: device [%s] not found",
                         nodeId_.c_str(), (long long)cmd.id, cmd.deviceId.c_str());
            }
        }
    }

    // 更新集群统计
    clusterMgr_.updateLocalStats(deviceMgr_.localOnlineCount(), cmdConsumed_);
}

// ============================================================
// sendCommandToDevice — 直接下发指令到设备
// ============================================================
bool GatewayServer::sendCommandToDevice(const Command& cmd) {
    auto* device = deviceMgr_.findDevice(cmd.deviceId);
    if (!device || !device->isOnline()) return false;

    std::string topic = cmd.topic.empty()
        ? ("/cmd/" + cmd.deviceId + "/down")
        : cmd.topic;

    static uint16_t s_packetId = 0;
    uint16_t packetId = ++s_packetId;
    if (packetId == 0) packetId = ++s_packetId;

    Buffer buf;
    MqttCodec::encodePublish(&buf, topic, cmd.payload,
                             MqttQoS::AT_LEAST_ONCE, packetId);
    device->connection->send(&buf);

    redisClient_.markCommandSent(cmd.id);
    redisClient_.ackCommand(cmd.streamId);

    LOG_INFO("[%s] Command [%lld] sent to device [%s]",
             nodeId_.c_str(), (long long)cmd.id, cmd.deviceId.c_str());
    return true;
}

// ============================================================
// processCrossNodeCommand — 处理其他节点转发的指令
//
// 当其他节点通过 Pub/Sub 将指令转发到本节点时,
// 由 ClusterManager 的订阅回调触发此方法
// ============================================================
void GatewayServer::processCrossNodeCommand(const Command& cmd) {
    LOG_INFO("[%s] Received cross-node command [%lld] for device [%s]",
             nodeId_.c_str(), (long long)cmd.id, cmd.deviceId.c_str());

    sendCommandToDevice(cmd);
}

// ============================================================
// enqueueCommand — 发布指令到 Streams (供 ApiServer 调用)
// ============================================================
int64_t GatewayServer::enqueueCommand(const std::string& deviceId,
                                      const std::string& command,
                                      const std::string& params) {
    int64_t cmdId = Timestamp::now().microSecondsSinceEpoch();

    Command cmd;
    cmd.id = cmdId;
    cmd.deviceId = deviceId;
    cmd.topic = "/cmd/" + deviceId + "/down";
    cmd.payload = "{\"command\":\"" + command + "\",\"params\":" + params + "}";
    cmd.timeout = 30;

    std::string streamId = redisClient_.publishCommand(cmd);
    LOG_INFO("[%s] Command [%lld] enqueued: device=%s cmd=%s streamId=%s",
             nodeId_.c_str(), (long long)cmdId, deviceId.c_str(),
             command.c_str(), streamId.c_str());

    return cmdId;
}
