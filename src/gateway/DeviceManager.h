#pragma once
#include "common/NonCopyable.h"
#include "net/TcpConnection.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <set>

class RedisClient;

// ============================================================
// 设备信息 (扩展版 — 支持分布式场景)
// ============================================================
struct DeviceInfo {
    std::string clientId;
    std::string username;
    std::string gatewayNodeId;  // 设备所属网关节点
    int64_t     lastHeartbeat;  // 最后一次心跳时间 (微秒时间戳)
    int64_t     onlineTime;     // 上线时间 (微秒时间戳)
    int         keepAlive;      // 保活时间 (秒)
    TcpConnectionPtr connection; // 对应的 TCP 连接

    // 设备元数据
    std::string deviceType;     // 设备类型 (sensor, actuator, gateway...)
    std::string firmwareVer;    // 固件版本
    std::string region;         // 部署区域

    bool isOnline() const { return connection && connection->connected(); }
};

// ============================================================
// DeviceManager — 分布式设备管理器
//
// 核心职责:
//  1. 管理本节点上的 deviceId → DeviceInfo 映射 (本地缓存)
//  2. 同步到 Redis 分布式注册表 (iot:devices Hash)
//  3. 管理 deviceId → subscribed topics 映射 (订阅关系)
//  4. 管理 topic → [deviceId] 倒排索引 (消息转发)
//  5. 检测心跳超时, 剔除离线设备 (本地 + Redis 同步)
//
// 分布式设计:
//  - 每个节点维护本地设备表 (内存, 快速访问)
//  - 设备上线 → 同时写入 Redis iot:devices (其他节点可见)
//  - 设备下线 → 同时删除 Redis 记录
//  - 心跳定时同步到 Redis (updateDeviceHeartbeat)
//  - 跨节点指令时, 先查本地 → 未命中则查 Redis → 转发
//
// 线程安全:
//  - 所有操作都在 EventLoop 线程中调用, 无需锁
//  - 保留了 mutex 用于未来可能的跨线程场景
// ============================================================
class DeviceManager : public NonCopyable {
public:
    DeviceManager();
    ~DeviceManager();

    // 设置 Redis 客户端 (用于分布式注册表同步)
    void setRedisClient(RedisClient* redis) { redis_ = redis; }
    // 设置本节点 ID
    void setNodeId(const std::string& nodeId) { nodeId_ = nodeId; }

    // ---- 设备生命周期 ----
    // 设备上线 (本地 + Redis 同步)
    void deviceOnline(const std::string& clientId,
                      const TcpConnectionPtr& conn,
                      int keepAlive,
                      const std::string& username = "",
                      const std::string& deviceType = "",
                      const std::string& firmwareVer = "");

    // 设备下线 (本地 + Redis 同步)
    void deviceOffline(const std::string& clientId);

    // 更新心跳 (本地 + Redis 同步)
    void updateHeartbeat(const std::string& clientId);

    // 查找设备 (仅查本地)
    DeviceInfo* findDevice(const std::string& clientId);

    // 查找设备所在节点 (本地 → Redis)
    std::string findDeviceNode(const std::string& clientId);

    // ---- 订阅管理 ----
    void subscribe(const std::string& clientId,
                   const std::string& topic, uint8_t qos);
    void unsubscribe(const std::string& clientId,
                     const std::string& topic);
    std::vector<TcpConnectionPtr> getSubscribers(const std::string& topic);

    // ---- 心跳超时检测 ----
    std::vector<std::string> checkHeartbeatTimeout();

    // ---- 统计 ----
    size_t onlineCount() const;
    size_t localOnlineCount() const { return devices_.size(); }
    std::vector<DeviceInfo> getAllLocalDevices() const;

private:
    RedisClient* redis_ = nullptr;
    std::string  nodeId_ = "unknown";

    // clientId → DeviceInfo (本节点的设备)
    std::unordered_map<std::string, DeviceInfo> devices_;

    // clientId → set<topic>
    std::unordered_map<std::string, std::set<std::string>> subscriptions_;

    // topic → set<clientId> (倒排索引, 用于消息 1:N 转发)
    std::unordered_map<std::string, std::set<std::string>> topicSubscribers_;

};
