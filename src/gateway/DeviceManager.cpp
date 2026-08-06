#include "DeviceManager.h"
#include "storage/RedisClient.h"
#include "common/Logger.h"
#include "common/Timestamp.h"

// ============================================================
// 内部辅助: 不加锁的下线操作
// ============================================================
static void deviceOfflineLocked(
    std::unordered_map<std::string, DeviceInfo>& devices,
    std::unordered_map<std::string, std::set<std::string>>& subscriptions,
    std::unordered_map<std::string, std::set<std::string>>& topicSubscribers,
    const std::string& clientId) {

    devices.erase(clientId);

    auto subIt = subscriptions.find(clientId);
    if (subIt != subscriptions.end()) {
        for (const auto& topic : subIt->second) {
            auto& subbers = topicSubscribers[topic];
            subbers.erase(clientId);
            if (subbers.empty()) {
                topicSubscribers.erase(topic);
            }
        }
        subscriptions.erase(subIt);
    }

    LOG_INFO("DeviceManager: device offline [%s]", clientId.c_str());
}

// ============================================================
// 构造与析构
// ============================================================
DeviceManager::DeviceManager() = default;
DeviceManager::~DeviceManager() = default;

// ============================================================
// 设备上线 — 本地注册 + Redis 分布式同步
// ============================================================
void DeviceManager::deviceOnline(const std::string& clientId,
                                 const TcpConnectionPtr& conn,
                                 int keepAlive,
                                 const std::string& username,
                                 const std::string& deviceType,
                                 const std::string& firmwareVer) {
    int64_t now = Timestamp::now().microSecondsSinceEpoch();

    DeviceInfo info;
    info.clientId = clientId;
    info.username = username;
    info.gatewayNodeId = nodeId_;
    info.keepAlive = keepAlive;
    info.lastHeartbeat = now;
    info.onlineTime = now;
    info.connection = conn;
    info.deviceType = deviceType;
    info.firmwareVer = firmwareVer;

    devices_[clientId] = info;

    // 同步到 Redis 分布式注册表 (非阻塞, 失败不影响本地)
    if (redis_ && redis_->isConnected()) {
        redis_->registerDevice(clientId, nodeId_, keepAlive);
    }

    LOG_INFO("DeviceManager: device online [%s] on node [%s], "
             "keepAlive=%ds, type=%s, local=%zu",
             clientId.c_str(), nodeId_.c_str(), keepAlive,
             deviceType.c_str(), devices_.size());
}

// ============================================================
// 设备下线 — 本地清理 + Redis 同步
// ============================================================
void DeviceManager::deviceOffline(const std::string& clientId) {
    deviceOfflineLocked(devices_, subscriptions_, topicSubscribers_, clientId);

    // 同步到 Redis
    if (redis_ && redis_->isConnected()) {
        redis_->unregisterDevice(clientId);
    }
}

// ============================================================
// 更新心跳 — 本地 + Redis
// ============================================================
void DeviceManager::updateHeartbeat(const std::string& clientId) {
    auto it = devices_.find(clientId);
    if (it != devices_.end()) {
        it->second.lastHeartbeat = Timestamp::now().microSecondsSinceEpoch();
    }

    // 定期同步到 Redis (每 ~5秒一次, 降低 Redis 负载)
    if (redis_ && redis_->isConnected()) {
        redis_->updateDeviceHeartbeat(clientId);
    }
}

// ============================================================
// 查找设备
// ============================================================
DeviceInfo* DeviceManager::findDevice(const std::string& clientId) {
    auto it = devices_.find(clientId);
    if (it != devices_.end() && it->second.isOnline()) {
        return &it->second;
    }
    return nullptr;
}

// ============================================================
// 查找设备所在节点 — 本地 → Redis
// ============================================================
std::string DeviceManager::findDeviceNode(const std::string& clientId) {
    // 先查本地
    auto it = devices_.find(clientId);
    if (it != devices_.end() && it->second.isOnline()) {
        return nodeId_;  // 设备在本节点
    }

    // 查 Redis 分布式注册表
    if (redis_ && redis_->isConnected()) {
        return redis_->getDeviceGateway(clientId);
    }

    return "";  // 设备不在线
}

// ============================================================
// 订阅管理
// ============================================================
void DeviceManager::subscribe(const std::string& clientId,
                              const std::string& topic,
                              uint8_t qos) {
    subscriptions_[clientId].insert(topic);
    topicSubscribers_[topic].insert(clientId);
    LOG_DEBUG("DeviceManager: [%s] subscribed to [%s] qos=%d",
              clientId.c_str(), topic.c_str(), qos);
}

void DeviceManager::unsubscribe(const std::string& clientId,
                               const std::string& topic) {
    auto subIt = subscriptions_.find(clientId);
    if (subIt != subscriptions_.end()) {
        subIt->second.erase(topic);
        if (subIt->second.empty()) subscriptions_.erase(subIt);
    }
    auto topicIt = topicSubscribers_.find(topic);
    if (topicIt != topicSubscribers_.end()) {
        topicIt->second.erase(clientId);
        if (topicIt->second.empty()) topicSubscribers_.erase(topicIt);
    }
}

std::vector<TcpConnectionPtr>
DeviceManager::getSubscribers(const std::string& topic) {
    std::vector<TcpConnectionPtr> result;
    auto it = topicSubscribers_.find(topic);
    if (it != topicSubscribers_.end()) {
        for (const auto& clientId : it->second) {
            auto devIt = devices_.find(clientId);
            if (devIt != devices_.end() && devIt->second.isOnline()) {
                result.push_back(devIt->second.connection);
            }
        }
    }
    return result;
}

// ============================================================
// 心跳超时检测 — 1.5倍 keepAlive 容忍度
// ============================================================
std::vector<std::string> DeviceManager::checkHeartbeatTimeout() {
    std::vector<std::string> timeoutDevices;
    int64_t now = Timestamp::now().microSecondsSinceEpoch();

    for (const auto& [clientId, info] : devices_) {
        double elapsed = (now - info.lastHeartbeat) / 1e6;
        if (elapsed > info.keepAlive * 1.5) {
            timeoutDevices.push_back(clientId);
            LOG_WARN("DeviceManager: heartbeat timeout [%s] elapsed=%.1fs "
                     "(keepAlive=%ds)", clientId.c_str(), elapsed, info.keepAlive);
        }
    }

    // 剔除超时设备
    for (const auto& id : timeoutDevices) {
        deviceOfflineLocked(devices_, subscriptions_, topicSubscribers_, id);
    }

    // 同步到 Redis
    if (redis_ && redis_->isConnected()) {
        for (const auto& id : timeoutDevices) {
            redis_->unregisterDevice(id);
        }
    }

    return timeoutDevices;
}

// ============================================================
// 统计
// ============================================================
size_t DeviceManager::onlineCount() const {
    if (redis_ && redis_->isConnected()) {
        return redis_->deviceCount();  // 分布式总数
    }
    return devices_.size();  // 仅本节点
}

std::vector<DeviceInfo> DeviceManager::getAllLocalDevices() const {
    std::vector<DeviceInfo> result;
    result.reserve(devices_.size());
    for (const auto& [id, info] : devices_) {
        result.push_back(info);
    }
    return result;
}
