#include "ClusterManager.h"
#include "net/EventLoop.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <random>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <unistd.h>

// ============================================================
// 生成节点 ID: hostname + 随机后缀
// ============================================================
std::string ClusterManager::generateNodeId() {
    char hostname[128] = {};
    if (::gethostname(hostname, sizeof(hostname)) != 0) {
        snprintf(hostname, sizeof(hostname), "gateway");
    }

    static std::mt19937 rng(static_cast<unsigned>(
        std::chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<int> dist(1000, 9999);

    char buf[256];
    snprintf(buf, sizeof(buf), "%s-%d", hostname, dist(rng));
    return buf;
}

// ============================================================
// 构造与析构
// ============================================================
ClusterManager::ClusterManager(EventLoop* loop, RedisClient* redis)
    : loop_(loop)
    , redis_(redis) {}

ClusterManager::~ClusterManager() {
    leave();
}

// ============================================================
// init — 初始化节点信息
// ============================================================
bool ClusterManager::init(const std::string& nodeId,
                          const std::string& addr,
                          uint16_t mqttPort, uint16_t apiPort) {
    nodeId_ = nodeId;
    localNode_.nodeId = nodeId;
    localNode_.addr = addr;
    localNode_.mqttPort = mqttPort;
    localNode_.apiPort = apiPort;
    localNode_.state = NodeState::STARTING;
    localNode_.lastHeartbeat = Timestamp::now().microSecondsSinceEpoch();

    LOG_INFO("ClusterManager: node [%s] initialized "
             "(mqtt=%u, api=%u, addr=%s)",
             nodeId_.c_str(), mqttPort, apiPort, addr.c_str());
    return true;
}

// ============================================================
// join — 加入集群
//
// 1. 在 Redis 注册本节点
// 2. 订阅本节点的跨节点路由通道
// 3. 发送首次心跳
// ============================================================
bool ClusterManager::join() {
    if (joined_) return true;
    if (!redis_ || !redis_->isConnected()) {
        LOG_ERROR("ClusterManager: cannot join — Redis not connected");
        return false;
    }

    // 注册节点
    bool ok = redis_->registerNode(nodeId_, localNode_.addr,
                                   localNode_.mqttPort, localNode_.apiPort);
    if (!ok) {
        LOG_ERROR("ClusterManager: failed to register node [%s]", nodeId_.c_str());
        return false;
    }

    // 发送初始心跳
    sendHeartbeat();

    // 发现现有节点
    discoverNodes();

    localNode_.state = NodeState::ONLINE;
    joined_ = true;

    LOG_INFO("ClusterManager: node [%s] joined cluster, "
             "%zu other nodes online",
             nodeId_.c_str(), knownNodes_.size());
    logClusterTopology();
    return true;
}

// ============================================================
// leave — 离开集群
// ============================================================
bool ClusterManager::leave() {
    if (!joined_) return true;

    stopHeartbeat();
    stopFailureDetection();

    if (redis_ && redis_->isConnected()) {
        // 清理节点注册信息
        // HDEL iot:nodes {nodeId}
        redis_->sendCommand(
            "HDEL iot:nodes " + nodeId_);
        redis_->sendCommand(
            "HDEL iot:nodes:heartbeat " + nodeId_);
    }

    localNode_.state = NodeState::OFFLINE;
    joined_ = false;

    LOG_INFO("ClusterManager: node [%s] left cluster", nodeId_.c_str());
    return true;
}

// ============================================================
// JSON 字段提取辅助函数
// ============================================================
std::string ClusterManager::extractJsonField(const std::string& json,
                                              const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    auto pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    auto end = json.find('"', pos);
    if (end == std::string::npos) return "";
    return json.substr(pos, end - pos);
}

// ============================================================
// ============================================================
void ClusterManager::startHeartbeat(double intervalSec) {
    if (!joined_) return;

    heartbeatTimerId_ = loop_->runEvery(intervalSec, [this]() {
        sendHeartbeat();
    });

    LOG_INFO("ClusterManager: heartbeat started (interval=%.1fs)", intervalSec);
}

void ClusterManager::stopHeartbeat() {
    if (heartbeatTimerId_ > 0) {
        loop_->cancelTimer(heartbeatTimerId_);
        heartbeatTimerId_ = 0;
    }
}

bool ClusterManager::sendHeartbeat() {
    if (!redis_ || !redis_->isConnected()) {
        // Redis 断连 → 降级模式
        if (localNode_.state == NodeState::ONLINE) {
            localNode_.state = NodeState::DEGRADED;
            LOG_WARN("ClusterManager: node [%s] entering DEGRADED mode "
                     "(Redis disconnected)", nodeId_.c_str());
        }
        return false;
    }

    // 恢复在线状态
    if (localNode_.state == NodeState::DEGRADED) {
        localNode_.state = NodeState::ONLINE;
        LOG_INFO("ClusterManager: node [%s] back to ONLINE", nodeId_.c_str());
    }

    bool ok = redis_->sendNodeHeartbeat(nodeId_);
    if (ok) {
        localNode_.lastHeartbeat = Timestamp::now().microSecondsSinceEpoch();
    }
    return ok;
}

// ============================================================
// 节点发现
// ============================================================
void ClusterManager::discoverNodes() {
    if (!redis_ || !redis_->isConnected()) return;

    auto onlineNodes = redis_->getOnlineNodes();
    std::vector<std::string> newNodes, lostNodes;

    // 找出新上线的节点
    for (auto& id : onlineNodes) {
        if (id == nodeId_) continue;  // 跳过自己
        if (std::find(knownNodes_.begin(), knownNodes_.end(), id)
            == knownNodes_.end()) {
            newNodes.push_back(id);
        }
    }

    // 找出下线的节点
    for (auto& id : knownNodes_) {
        if (id == nodeId_) continue;
        if (std::find(onlineNodes.begin(), onlineNodes.end(), id)
            == onlineNodes.end()) {
            lostNodes.push_back(id);
        }
    }

    // 通知新节点
    for (auto& id : newNodes) {
        auto info = redis_->getNodeInfo(id);
        LOG_INFO("ClusterManager: new node detected: [%s] info=%s",
                 id.c_str(), info.c_str());
        if (nodeChangeCb_) {
            ClusterNode node;
            node.nodeId = id;
            nodeChangeCb_(node, true);
        }
    }

    // 通知丢失节点
    for (auto& id : lostNodes) {
        LOG_WARN("ClusterManager: node lost: [%s]", id.c_str());
        if (nodeChangeCb_) {
            ClusterNode node;
            node.nodeId = id;
            nodeChangeCb_(node, false);
        }
    }

    // 更新缓存
    knownNodes_ = std::move(onlineNodes);
}

std::vector<ClusterNode> ClusterManager::getOnlineNodes() {
    std::vector<ClusterNode> result;
    result.push_back(localNode_);

    if (redis_ && redis_->isConnected()) {
        auto nodeIds = redis_->getOnlineNodes();
        for (auto& id : nodeIds) {
            if (id == nodeId_) continue;
            auto info = redis_->getNodeInfo(id);
            ClusterNode node;
            node.nodeId = id;
            node.addr = extractJsonField(info, "addr");
            node.state = NodeState::ONLINE;
            result.push_back(node);
        }
    }
    return result;
}

ClusterStats ClusterManager::getStats() {
    ClusterStats stats;
    stats.totalNodes = 1;
    stats.onlineNodes = joined_ ? 1 : 0;
    stats.totalDevices = localDeviceCount_;

    if (redis_ && redis_->isConnected()) {
        stats.totalNodes = knownNodes_.size() + 1;
        stats.onlineNodes = knownNodes_.size() + (joined_ ? 1 : 0);
        stats.totalDevices = redis_->deviceCount();
        stats.pendingCommands = static_cast<size_t>(redis_->pendingCount());
    }

    return stats;
}

void ClusterManager::updateLocalStats(size_t deviceCount, size_t cmdConsumed) {
    localDeviceCount_ = deviceCount;
    localCmdConsumed_ = cmdConsumed;
}

// ============================================================
// 跨节点路由
//
// 核心流程:
//  1. 本节点收到指令, 目标设备 clientId
//  2. 查本地 DeviceManager → 如果设备在本节点, 直接下发
//  3. 如果不在本节点, 查 Redis 分布式注册表 → 找到目标节点
//  4. 通过 Pub/Sub 将指令转发到目标节点
//  5. 目标节点收到转发指令, 下发给设备
//
// 注意:
//  - Pub/Sub 是即发即忘的, 不保证投递
//  - 如果需要可靠性保证, 应使用 Streams 做降级
// ============================================================
void ClusterManager::subscribeRouteChannel(
    RedisClient::CrossNodeCmdCallback cb) {
    if (!redis_ || !redis_->isConnected()) return;
    redis_->subscribeRouteChannel(nodeId_, std::move(cb));
}

bool ClusterManager::forwardCommand(const std::string& clientId,
                                    const Command& cmd) {
    if (!redis_ || !redis_->isConnected()) return false;

    // 查设备所在节点
    std::string targetNode = redis_->getDeviceGateway(clientId);
    if (targetNode.empty()) {
        LOG_WARN("ClusterManager: device [%s] not found in distributed registry",
                 clientId.c_str());
        return false;
    }

    // 解析 JSON 获取 gateway 字段
    std::string parsedNode = extractJsonField(targetNode, "gateway");
    if (!parsedNode.empty()) {
        targetNode = parsedNode;
    }

    if (targetNode == nodeId_) {
        // 设备在本节点 (异常, 调用方应先检查)
        return false;
    }

    LOG_INFO("ClusterManager: forwarding command [%lld] for device [%s] "
             "to node [%s]",
             (long long)cmd.id, clientId.c_str(), targetNode.c_str());

    return redis_->publishToNode(targetNode, cmd);
}

// ============================================================
// 故障检测
//
// 每 checkIntervalSec 秒扫描:
//  1. 获取 Redis 中所有有心跳的节点
//  2. 对比本地 knownNodes_ 缓存
//  3. 发现新增/丢失
//  4. 对丢失节点触发故障转移
// ============================================================
void ClusterManager::startFailureDetection(double checkIntervalSec) {
    if (!joined_) return;

    failureDetectTimerId_ = loop_->runEvery(checkIntervalSec, [this]() {
        auto failed = detectFailedNodes();
        if (!failed.empty()) {
            LOG_WARN("ClusterManager: detected %zu failed nodes", failed.size());
            for (auto& nodeId : failed) {
                LOG_WARN("  - failed node: [%s]", nodeId.c_str());
                // 尝试认领其未 ACK 消息
                size_t claimed = claimPendingMessages(nodeId, 30000);
                if (claimed > 0) {
                    LOG_INFO("ClusterManager: claimed %zu pending messages "
                             "from failed node [%s]", claimed, nodeId.c_str());
                }
            }
        }
        // 同时也做一次节点发现
        discoverNodes();
    });

    LOG_INFO("ClusterManager: failure detection started (interval=%.1fs)",
             checkIntervalSec);
}

void ClusterManager::stopFailureDetection() {
    if (failureDetectTimerId_ > 0) {
        loop_->cancelTimer(failureDetectTimerId_);
        failureDetectTimerId_ = 0;
    }
}

std::vector<std::string> ClusterManager::detectFailedNodes() {
    std::vector<std::string> failed;
    if (!redis_ || !redis_->isConnected()) return failed;

    auto aliveNodes = redis_->getOnlineNodes();

    for (auto& knownNode : knownNodes_) {
        if (knownNode == nodeId_) continue;
        if (std::find(aliveNodes.begin(), aliveNodes.end(), knownNode)
            == aliveNodes.end()) {
            // 节点心跳丢失超过15秒 → 判定为故障
            failed.push_back(knownNode);
        }
    }

    return failed;
}

// ============================================================
// 故障转移 — 认领故障节点的未 ACK 消息
//
// XCLAIM 语法:
//   XCLAIM <stream> <group> <consumer> <min-idle-time> <id-1> ... <id-N>
//
// 工作原理:
//  1. 先用 XPENDING 查询故障节点在消费者组中未 ACK 的消息
//  2. 筛选闲置超过 minIdleMs 的消息
//  3. 用 XCLAIM 将这些消息归属到本节点
//  4. 本节点的消费者可以重新处理这些消息
// ============================================================
size_t ClusterManager::claimPendingMessages(const std::string& failedNodeId,
                                           int64_t minIdleMs) {
    if (!redis_ || !redis_->isConnected()) return 0;

    // XPENDING <key> <group> - + <count> <consumer>
    // 查询故障节点的未确认消息
    std::string result = redis_->sendCommand(
        "XPENDING device:commands gateway-group - + 100 " + failedNodeId);

    // 简化: 日志记录, 实际 XCLAIM 实现依赖 hiredis 异步 API
    LOG_INFO("ClusterManager: claiming pending messages from [%s], "
             "min_idle=%lldms", failedNodeId.c_str(), (long long)minIdleMs);

    // 完整实现:
    // 1. 解析 XPENDING 返回的消息 ID 列表
    // 2. XCLAIM device:commands gateway-group {nodeId_} {minIdleMs} {ids...}
    // 3. 收到认领的消息后, 重新处理 (processCommand)

    return 0;
}

// ============================================================
// 集群拓扑日志
// ============================================================
void ClusterManager::logClusterTopology() {
    auto nodes = getOnlineNodes();
    size_t totalDevices = redis_ ? redis_->deviceCount() : 0;

    LOG_INFO("========================================");
    LOG_INFO(" Cluster Topology");
    LOG_INFO("========================================");
    LOG_INFO(" This node:  [%s] (%s:%u)", nodeId_.c_str(),
             localNode_.addr.c_str(), localNode_.mqttPort);
    LOG_INFO(" Online:     %zu nodes", nodes.size());
    LOG_INFO(" Devices:    %zu total (distributed registry)", totalDevices);

    for (auto& node : nodes) {
        const char* marker = (node.nodeId == nodeId_) ? " ← self" : "";
        LOG_INFO("  - [%s] %s:%u%s",
                 node.nodeId.c_str(), node.addr.c_str(),
                 node.mqttPort, marker);
    }
    LOG_INFO("========================================");
}
