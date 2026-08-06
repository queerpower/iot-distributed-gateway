#pragma once
#include "common/NonCopyable.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <cstdint>
#include <atomic>
#include <memory>

class EventLoop;

// ============================================================
// Redis 配置
// ============================================================
struct RedisConfig {
    std::string host = "127.0.0.1";
    int         port = 6379;
    std::string password = "";
    int         db = 0;

    // Streams 配置
    std::string streamKey = "device:commands";
    std::string consumerGroup = "gateway-group";
    std::string consumerName = "gateway-1";  // 启动时动态覆盖

    // 连接池配置
    int         poolSize = 4;           // 连接池大小
    int         connTimeoutMs = 3000;   // 连接超时
    int         cmdTimeoutMs = 5000;    // 命令超时
};

// ============================================================
// 指令结构体
// ============================================================
struct Command {
    int64_t     id;             // 指令 ID (毫秒时间戳)
    std::string deviceId;       // 目标设备
    std::string topic;          // 下发 topic
    std::string payload;        // 指令内容 (JSON)
    int         timeout;        // 超时 (秒)
    std::string streamId;       // Redis Stream 消息 ID (用于 XACK)
    int64_t     createdAt;      // 创建时间 (微秒)
};

// ============================================================
// RedisClient — 基于 hiredis 异步 API 的 Redis 客户端
//
// 核心职责:
//  1. 连接管理 (连接池 + 自动重连)
//  2. XREADGROUP 消费 Streams 中的指令 (消费者组模式)
//  3. XACK 确认消费 (at-least-once 语义)
//  4. 分布式设备注册表 (Hash: device_id → gateway_node)
//  5. 跨节点消息路由 (Pub/Sub: gateway:cross-node)
//  6. 指令状态追踪 (Sorted Set: 超时扫描)
//  7. 网关节点注册与心跳 (Hash + TTL)
//
// 设计决策 — 为什么用 Redis Streams 消费者组:
//  1. 多网关节点并行消费, Redis 自动负载均衡
//  2. XREADGROUP + XACK 保证 at-least-once 投递
//  3. XPENDING 可查询未确认消息, 支持超时重试
//  4. 消费者组内消息不会重复投递给不同消费者
//  5. 网关节点崩溃后, 未 ACK 的消息可被其他节点认领
//
// 分布式设备注册表设计:
//  - Key: iot:devices (Hash) — device_id → JSON{gateway, heartbeat, ...}
//  - 设备上线: HSET iot:devices {device_id} {json}
//  - 设备下线: HDEL iot:devices {device_id}
//  - 查找设备所在节点: HGET iot:devices {device_id}
//
// 跨节点指令路由:
//  - 当指令目标设备不在本节点时, 通过 Pub/Sub 转发到目标节点
//  - Channel: iot:route:{gateway_node_id}
//  - 每个节点订阅自己的 channel 接收跨节点指令
// ============================================================
class RedisClient : public NonCopyable {
public:
    using CommandCallback = std::function<void(const Command& cmd)>;
    using CrossNodeCmdCallback = std::function<void(const Command& cmd)>;

    RedisClient(EventLoop* loop, const RedisConfig& cfg);
    ~RedisClient();

    // ---- 生命周期 ----
    bool connect();
    void disconnect();
    bool isConnected() const { return connected_; }
    bool reconnect();

    // ---- Redis Streams 消费者组 ----
    // 初始化消费者组 (幂等: 已存在则忽略)
    bool initConsumerGroup();
    // 从 Streams 消费指令 (非阻塞轮询模式)
    // 通过 consumer group 实现多节点负载均衡
    std::vector<Command> consumeCommands(int batchSize = 10, int blockMs = 0);
    // 确认消费 (XACK)
    bool ackCommand(const std::string& streamId);
    // 查询待处理消息数
    int64_t pendingCount();

    // ---- 指令生命周期 ----
    // 发布指令到 Streams (API 调用)
    std::string publishCommand(const Command& cmd);
    // 标记指令已发送到设备
    bool markCommandSent(int64_t commandId);
    // 标记指令失败 (设备离线/超时)
    bool markCommandFailed(int64_t commandId, const std::string& reason);
    // 标记指令完成 (设备已 ACK)
    bool markCommandCompleted(int64_t commandId, const std::string& response);
    // 查询指令状态
    std::string getCommandStatus(int64_t commandId);
    // 扫描超时指令
    std::vector<int64_t> scanTimeoutCommands(int64_t timeoutUs);

    // ---- 分布式设备注册表 ----
    bool registerDevice(const std::string& clientId,
                       const std::string& gatewayNodeId,
                       int keepAlive = 60);
    bool unregisterDevice(const std::string& clientId);
    std::string getDeviceGateway(const std::string& clientId);
    std::vector<std::pair<std::string, std::string>> getAllDevices();
    size_t deviceCount();
    bool updateDeviceHeartbeat(const std::string& clientId);

    // ---- 网关节点管理 ----
    // 注册本节点
    bool registerNode(const std::string& nodeId, const std::string& addr,
                     uint16_t mqttPort, uint16_t apiPort);
    // 发送心跳 (维持 TTL)
    bool sendNodeHeartbeat(const std::string& nodeId);
    // 获取所有在线节点
    std::vector<std::string> getOnlineNodes();
    // 获取节点信息
    std::string getNodeInfo(const std::string& nodeId);

    // ---- 跨节点消息路由 (Pub/Sub) ----
    // 订阅本节点的路由 channel
    bool subscribeRouteChannel(const std::string& nodeId,
                              CrossNodeCmdCallback cb);
    // 向指定节点的路由 channel 发送指令 (转发到目标设备所在节点)
    bool publishToNode(const std::string& targetNodeId,
                      const Command& cmd);

    // ---- 设备遥测数据缓存 ----
    // 缓存设备最新遥测数据 (Hash: iot:telemetry:{device_id})
    bool cacheTelemetry(const std::string& clientId,
                       const std::string& topic,
                       const std::string& payload);

    // ---- 统计信息 ----
    // 获取 Streams 信息
    int64_t streamLength();
    // 获取消费者组信息
    std::string consumerGroupInfo();

    void setConsumerName(const std::string& name) {
        config_.consumerName = name;
    }

    // 配置访问器
    const RedisConfig& getConfig() const { return config_; }

    // 底层命令执行 (供 ClusterManager 等内部模块使用)
    std::string sendCommand(const std::string& cmd);

private:
    EventLoop*     loop_;          // 所属的事件循环线程
    RedisConfig    config_;        // 配置
    std::atomic<bool> connected_;  // 连接状态（原子变量，线程安全）


    // 连接池 (主连接 + 订阅连接)
    struct RedisConnection {
        void* ctx = nullptr;  // redisContext*
        int64_t lastUsed = 0;
        bool inUse = false;
    };
    RedisConnection  conn_;       // 主连接 (命令执行)
    RedisConnection  subConn_;    // 订阅连接 (Pub/Sub 接收)
    bool             subActive_ = false;

    // 跨节点指令回调
    CrossNodeCmdCallback routeCallback_;

};
