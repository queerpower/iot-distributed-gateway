#pragma once
#include "common/NonCopyable.h"
#include "net/Callbacks.h"
#include "DeviceManager.h"
#include "ClusterManager.h"
#include "protocol/MqttCodec.h"
#include "storage/RedisClient.h"
#include "storage/TsdbClient.h"
#include <memory>
#include <atomic>
#include <string>

class EventLoop;
class TcpServer;

// ============================================================
// GatewayServer — 分布式IoT设备网关核心
//
// 职责:
//  1. 接受设备 MQTT 连接, 解析报文 (多Reactor)
//  2. 设备认证 (CONNECT) 与心跳维护 (PINGREQ)
//  3. 设备数据上报 → 写入 TSDB + Redis 遥测缓存
//  4. 主题订阅/发布消息转发 (同节点 1:N 转发)
//  5. 从 Redis Streams 消费者组消费指令 (XREADGROUP, 多节点竞争)
//  6. 跨节点指令路由 (通过 ClusterManager → Pub/Sub)
//  7. 集群节点健康管理与故障检测
//
// 核心数据流 (分布式版):
//
//   设备 ──MQTT──→ Nginx L4 LB ──TCP──→ Gateway-1/Gateway-2/Gateway-N
//                                           │
//                      ┌────────────────────┼────────────────────┐
//                      │                    │                    │
//               ┌──────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐
//               │ DeviceMgr   │    │ Redis Streams │    │  TSDB        │
//               │ (本地+Redis)│    │ (消费者组)     │    │  (批量写入)   │
//               └──────┬──────┘    └───────┬──────┘    └──────────────┘
//                      │                    │
//               ┌──────▼──────┐    ┌───────▼──────┐
//               │ClusterMgr   │    │ 跨节点路由     │
//               │(节点发现)    │    │ (Pub/Sub)     │
//               └─────────────┘    └──────────────┘
//
//  指令下发链路:
//  API → XADD Redis Streams → XREADGROUP (消费者组, 自动负载均衡)
//      → Gateway-N 消费 → 查本地设备表
//          ├── 设备在本节点 → MQTT PUBLISH 下发
//          └── 设备在其他节点 → Pub/Sub 转发到目标节点
//
//  消费者组负载均衡:
//  - gateway-1, gateway-2, gateway-3 同属 consumer group "gateway-group"
//  - 每条 Streams 消息只会投递给组内一个消费者
//  - Redis 自动在组内消费者间轮转分配
//  - 某节点宕机 → 其未ACK消息可被其他节点 XCLAIM 认领
//
// ============================================================
class GatewayServer : public NonCopyable {
public:
    GatewayServer(EventLoop* loop, uint16_t mqttPort,
                  const RedisConfig& redisCfg,
                  const TsdbConfig& tsdbCfg);
    ~GatewayServer();

    // ---- 配置 ----
    void setThreadNum(int num);
    void setNodeId(const std::string& nodeId);
    // ---- 生命周期 ----
    void start();
    void stop();

    // ---- 统计 ----
    size_t deviceCount() const { return deviceMgr_.onlineCount(); }
    size_t localDeviceCount() const { return deviceMgr_.localOnlineCount(); }
    // ---- 指令下发 (公开, 供 ApiServer 注入) ----
    int64_t enqueueCommand(const std::string& deviceId,
                          const std::string& command,
                          const std::string& params);

    // 接入器
    ClusterStats clusterStats() { return clusterMgr_.getStats(); }
    int64_t consumedCommandCount() const { return cmdConsumed_.load(); }
    int64_t forwardedCommandCount() const { return cmdForwarded_.load(); }

private:
    // ---- MQTT 连接回调 ----
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts);
    void onDisconnect(const TcpConnectionPtr& conn);

    // ---- MQTT 报文处理 ----
    void handleConnect(const TcpConnectionPtr& conn,
                       const MqttConnectPayload& payload);
    void handlePublish(const TcpConnectionPtr& conn,
                       const std::string& topic, const std::string& payload,
                       MqttQoS qos, bool retain, uint16_t packetId);
    void handleSubscribe(const TcpConnectionPtr& conn,
                        uint16_t packetId,
                        const std::vector<MqttTopicFilter>& filters);
    void handlePingReq(const TcpConnectionPtr& conn);

    // ---- 指令下发 (分布式) ----
    void processCommand();                         // 从 Redis Streams 消费指令
    bool sendCommandToDevice(const Command& cmd);  // 下发指令到设备
    void processCrossNodeCommand(const Command& cmd); // 处理跨节点转发的指令

    // ---- 后台任务 ----
    void startHeartbeatCheck();
    void startCommandConsumer();

    // ---- 指令生成 (供 ApiServer 调用) ----
    // (moved to public: section above)

    EventLoop*    loop_;
    uint16_t      mqttPort_;
    std::atomic<bool> running_;
    std::string   nodeId_;

    // 存储 — 必须在 clusterMgr_ 之前构造 (ClusterManager 持有 RedisClient*)
    RedisClient redisClient_;
    TsdbClient  tsdbClient_;

    // 网络
    std::unique_ptr<TcpServer> tcpServer_;
    MqttCodec       codec_;
    TcpConnectionPtr currentConn_;  // 当前处理的连接

    // 业务 — deviceMgr_ 也需要 RedisClient* (在 start() 中设置)
    DeviceManager   deviceMgr_;
    ClusterManager  clusterMgr_;   // 依赖 redisClient_ 已构造

    // 指令计数
    std::atomic<int64_t> cmdConsumed_{0};
    std::atomic<int64_t> cmdForwarded_{0};

};
