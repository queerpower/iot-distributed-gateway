#pragma once
#include "common/NonCopyable.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"
#include "net/Callbacks.h"
#include <memory>
#include <functional>

class EventLoop;

// ============================================================
// ApiServer — HTTP API 服务器 (指令下发 + 集群管理入口)
//
// 功能:
//   POST /api/v1/commands       — 下发指令到设备 (→ Redis Streams)
//   GET  /api/v1/devices         — 查询在线设备列表 (分布式注册表)
//   GET  /api/v1/device/:id      — 查询指定设备详情
//   GET  /api/v1/commands/:id    — 查询指令执行状态
//   GET  /api/v1/stats           — 网关统计信息
//   GET  /api/v1/cluster         — 集群拓扑信息 (分布式)
//   GET  /api/v1/nodes           — 集群节点列表
//   GET  /api/v1/health          — 节点健康检查
//
// 指令下发流程:
//   1. HTTP API 收到指令请求
//   2. 校验参数 (device_id, command, params)
//   3. 将指令写入 Redis Streams (XADD)
//   4. 返回指令 ID 给调用方
//   5. 网关节点通过 XREADGROUP 竞争消费指令
//   6. 目标节点查设备所在位置 → 本节点直接下发 / 转 PubSub 到目标节点
//   7. 设备执行后回复 ACK, 网关更新指令状态
// ============================================================
class ApiServer : public NonCopyable {
public:
    using DeviceCountFn = std::function<size_t()>;
    using CommandSenderFn = std::function<int64_t(
        const std::string& deviceId,
        const std::string& command,
        const std::string& params)>;
    using ClusterStatsFn = std::function<std::string()>;
    using DeviceDetailFn = std::function<std::string(const std::string&)>;
    using CommandStatusFn = std::function<std::string(int64_t)>;

    ApiServer(EventLoop* loop, uint16_t port);
    ~ApiServer();

    void setThreadNum(int num);

    // 注入业务逻辑回调
    void setDeviceCountFn(DeviceCountFn fn) { deviceCountFn_ = std::move(fn); }
    void setCommandSenderFn(CommandSenderFn fn) { commandSenderFn_ = std::move(fn); }
    void setClusterStatsFn(ClusterStatsFn fn) { clusterStatsFn_ = std::move(fn); }
    void setDeviceDetailFn(DeviceDetailFn fn) { deviceDetailFn_ = std::move(fn); }
    void setCommandStatusFn(CommandStatusFn fn) { commandStatusFn_ = std::move(fn); }

    void start();
    void stop();

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts);

    // HTTP 请求处理
    void handleHttpRequest(const TcpConnectionPtr& conn, Buffer* buf);

    // 路由处理器
    void handlePostCommand(const TcpConnectionPtr& conn, const std::string& body);
    void handleGetDevices(const TcpConnectionPtr& conn);
    void handleGetDeviceDetail(const TcpConnectionPtr& conn,
                              const std::string& deviceId);
    void handleGetCommandStatus(const TcpConnectionPtr& conn, int64_t commandId);
    void handleGetStats(const TcpConnectionPtr& conn);
    void handleGetCluster(const TcpConnectionPtr& conn);
    void handleGetNodes(const TcpConnectionPtr& conn);
    void handleGetHealth(const TcpConnectionPtr& conn);
    void handleNotFound(const TcpConnectionPtr& conn);

    // HTTP 响应工具
    void sendHttpResponse(const TcpConnectionPtr& conn,
                         int statusCode,
                         const std::string& contentType,
                         const std::string& body);
    void sendJsonResponse(const TcpConnectionPtr& conn,
                         int statusCode, const std::string& json);

    EventLoop* loop_;
    uint16_t   port_;
    std::unique_ptr<TcpServer> tcpServer_;

    DeviceCountFn    deviceCountFn_;
    CommandSenderFn  commandSenderFn_;
    ClusterStatsFn   clusterStatsFn_;
    DeviceDetailFn   deviceDetailFn_;
    CommandStatusFn  commandStatusFn_;
};
