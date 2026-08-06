#pragma once
#include "common/NonCopyable.h"
#include "storage/RedisClient.h"
#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <memory>

class EventLoop;

// ============================================================
// 节点状态
// ============================================================
enum class NodeState {
    STARTING,
    ONLINE,
    DEGRADED,   // 部分功能不可用 (如 Redis 断开)
    OFFLINE,
};

// ============================================================
// 集群节点信息
// ============================================================
struct ClusterNode {
    std::string nodeId;
    std::string addr;
    uint16_t    mqttPort = 0;
    uint16_t    apiPort = 0;
    NodeState   state = NodeState::OFFLINE;
    int64_t     lastHeartbeat = 0;  // 微秒
    size_t      deviceCount = 0;
    size_t      cmdConsumed = 0;    // 累计消费指令数
};

// ============================================================
// 集群统计
// ============================================================
struct ClusterStats {
    size_t totalNodes = 0;
    size_t onlineNodes = 0;
    size_t totalDevices = 0;
    size_t pendingCommands = 0;
    int64_t uptimeSeconds = 0;
};

// ============================================================
// ClusterManager — 分布式集群管理器
//
// 核心职责:
//  1. 节点注册与发现 (Redis Hash + Heartbeat TTL)
//  2. 节点健康检查 (定时心跳, 超时剔除)
//  3. 跨节点指令路由 (查分布式设备注册表 → Pub/Sub 转发)
//  4. 集群拓扑感知 (哪些节点在线, 各自负载)
//  5. 故障转移 (节点宕机后, 其未 ACK 指令可被其他节点认领)
//
// 集群协调机制:
//  ┌──────────────────────────────────────────────────────┐
//  │                   Redis (共享状态)                    │
//  │  iot:nodes — 节点注册表 (Hash)                       │
//  │  iot:nodes:heartbeat — 心跳集合 (TTL Key)            │
//  │  iot:devices — 分布式设备注册表 (Hash)                │
//  │  iot:route:{nodeId} — 跨节点路由通道 (Pub/Sub)       │
//  │  device:commands — 指令流 (Streams, 消费者组)        │
//  └──┬───────────────┬───────────────┬──────────────────┘
//     │               │               │
//  ┌──▼──────┐   ┌───▼──────┐   ┌───▼──────┐
//  │Gateway-1│   │Gateway-2 │   │Gateway-3 │
//  │ :1883   │   │ :1884   │   │ :1885   │
//  └──┬──────┘   └───┬──────┘   └───┬──────┘
//     │               │               │
//     └───────────────┼───────────────┘
//                     │
//          ┌──────────▼──────────┐
//          │  Nginx L4 LB :1883  │  ← 设备统一连接入口
//          └─────────────────────┘
//
// 心跳机制:
//  - 每 5 秒发送一次心跳 (HSET + EXPIRE)
//  - 如果 EXPIRE 超时 (15秒), key 被 Redis 自动删除
//  - 其他节点通过检查 iot:nodes:heartbeat 判断节点存活
//
// 故障转移流程:
//  1. 节点 A 检测到节点 B 心跳丢失
//  2. 节点 A 查询 iot:devices 中归属节点 B 的设备
//  3. 认领节点 B 在 Streams 中的未 ACK 消息 (XCLAIM)
//  4. 标记这些设备为离线 (设备需重新连接)
// ============================================================
class ClusterManager : public NonCopyable {
public:
    using NodeChangeCallback = std::function<void(const ClusterNode& node, bool online)>;

    ClusterManager(EventLoop* loop, RedisClient* redis);
    ~ClusterManager();

    // ---- 节点生命周期 ----
    // 初始化节点 (生成 nodeId, 注册到 Redis)
    bool init(const std::string& nodeId, const std::string& addr,
              uint16_t mqttPort, uint16_t apiPort);
    // 加入集群 (注册 + 订阅路由通道)
    bool join();
    // 离开集群 (清理注册信息)
    bool leave();
    // 是否已加入集群
    bool isJoined() const { return joined_; }

    // ---- 心跳 ----
    // 启动定时心跳 (每 heartbeatIntervalSec 秒)
    void startHeartbeat(double heartbeatIntervalSec = 5.0);
    // 停止心跳
    void stopHeartbeat();
    // 手动发送一次心跳
    bool sendHeartbeat();

    // ---- 节点发现 ----
    // 获取所有在线节点
    std::vector<ClusterNode> getOnlineNodes();
    // 获取本节点信息
    const ClusterNode& localNode() const { return localNode_; }
    // 获取集群统计
    ClusterStats getStats();
    // 统计本节点
    void updateLocalStats(size_t deviceCount, size_t cmdConsumed);

    // ---- 跨节点路由 ----
    // 订阅路由通道 (接收其他节点转发的指令)
    void subscribeRouteChannel(RedisClient::CrossNodeCmdCallback cb);
    // 转发指令到设备所在节点 (设备不在本节点时)
    bool forwardCommand(const std::string& clientId, const Command& cmd);

    // ---- 故障检测 ----
    // 启动故障检测 (每 checkIntervalSec 扫描)
    void startFailureDetection(double checkIntervalSec = 10.0);
    // 停止故障检测
    void stopFailureDetection();
    // 手动检测故障节点
    std::vector<std::string> detectFailedNodes();

    // ---- 故障转移 ----
    // 认领故障节点的未 ACK 消息
    // 注意: XCLAIM 只能在同消费者组内认领
    // 实际上所有节点在同一消费者组, 所以故障节点的 PEL 消息
    // 可由其他存活节点通过 XCLAIM 认领
    size_t claimPendingMessages(const std::string& failedNodeId,
                               int64_t minIdleMs = 30000);

    // ---- 事件回调 ----
    void setNodeChangeCallback(NodeChangeCallback cb) {
        nodeChangeCb_ = std::move(cb);
    }

    // ---- 工具 ----
    static std::string generateNodeId();
    std::string nodeId() const { return nodeId_; }
    static std::string extractJsonField(const std::string& json, const std::string& key);

private:
    // 定期节点发现
    void discoverNodes();
    // 打印集群拓扑
    void logClusterTopology();

    EventLoop*   loop_;
    RedisClient* redis_;
    std::string  nodeId_;
    ClusterNode  localNode_;
    std::atomic<bool> joined_{false};

    // 心跳定时器 ID
    int64_t heartbeatTimerId_ = 0;
    // 故障检测定时器 ID
    int64_t failureDetectTimerId_ = 0;

    // 已知的在线节点 (缓存)
    std::vector<std::string> knownNodes_;
    // 本地统计
    std::atomic<size_t> localDeviceCount_{0};
    std::atomic<size_t> localCmdConsumed_{0};

    // 回调
    NodeChangeCallback nodeChangeCb_;
};
