#pragma once
#include "common/NonCopyable.h"
#include "net/Callbacks.h"
#include "net/TcpConnection.h"
#include "net/Buffer.h"
#include <string>
#include <functional>
#include <memory>

class EventLoop;
class TcpServer;
class RedisClient;

// ============================================================
// WebDashboard — 设备监控管理看板
//
// 功能:
//  1. 仪表盘首页 (集群概览, 设备统计, 指令统计)
//  2. 设备列表页 (在线/离线/全部, 分页)
//  3. 设备详情页 (遥测数据, 指令历史, 连接信息)
//  4. 指令调度页 (下发指令, 批量指令)
//  5. 告警中心 (告警列表, 规则配置)
//  6. 集群拓扑页 (节点状态, 负载分布)
//
// 前端: 内嵌 HTML/CSS/JS (单页应用)
// 后端 API: 复用 ApiServer 的 REST 接口
//
// 访问: http://{host}:{port}/dashboard
// ============================================================
class WebDashboard : public NonCopyable {
public:
    WebDashboard(EventLoop* loop, uint16_t port, RedisClient* redis);
    ~WebDashboard();

    void setThreadNum(int num);
    void start();
    void stop();

private:
    // HTTP 处理
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts);
    void handleHttpRequest(const TcpConnectionPtr& conn, Buffer* buf);

    // 页面路由
    void serveIndex(const TcpConnectionPtr& conn);
    void serveApiDevices(const TcpConnectionPtr& conn);
    void serveApiCluster(const TcpConnectionPtr& conn);
    void serveApiAlerts(const TcpConnectionPtr& conn);
    void serveNotFound(const TcpConnectionPtr& conn);

    // HTTP 工具
    void sendHttpResponse(const TcpConnectionPtr& conn,
                         int statusCode, const std::string& contentType,
                         const std::string& body);
    void sendHtml(const TcpConnectionPtr& conn,
                  int statusCode, const std::string& html);

    // 内嵌 HTML 页面
    std::string buildDashboardHtml() const;
    std::string buildDevicesHtml() const;
    std::string buildAlertsHtml() const;

    EventLoop* loop_;
    uint16_t port_;
    RedisClient* redis_;
    std::unique_ptr<TcpServer> tcpServer_;
};
