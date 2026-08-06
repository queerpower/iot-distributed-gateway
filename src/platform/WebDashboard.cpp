#include "WebDashboard.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "storage/RedisClient.h"
#include "common/Logger.h"
#include <sstream>

WebDashboard::WebDashboard(EventLoop* loop, uint16_t port, RedisClient* redis)
    : loop_(loop), port_(port), redis_(redis) {
    auto addr = InetAddress(port);
    tcpServer_ = std::make_unique<TcpServer>(loop, addr, "WebDashboard");
}

WebDashboard::~WebDashboard() = default;

void WebDashboard::setThreadNum(int num) { tcpServer_->setThreadNum(num); }

void WebDashboard::start() {
    tcpServer_->setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { onConnection(conn); });
    tcpServer_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            onMessage(conn, buf, ts);
        });
    tcpServer_->start();
    LOG_INFO("WebDashboard started on http://0.0.0.0:%u/dashboard", port_);
}

void WebDashboard::stop() {}

void WebDashboard::onConnection(const TcpConnectionPtr& conn) {
    if (!conn->connected()) return;
}

void WebDashboard::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
    handleHttpRequest(conn, buf);
}

void WebDashboard::handleHttpRequest(const TcpConnectionPtr& conn, Buffer* buf) {
    const char* crlf = buf->findCRLF();
    if (!crlf) return;

    std::string line(buf->peek(), crlf - buf->peek());
    buf->retrieveUntil(crlf + 2);

    std::istringstream iss(line);
    std::string method, path, version;
    iss >> method >> path >> version;

    // 跳过 headers
    while (true) {
        const char* end = buf->findCRLF();
        if (!end) return;
        std::string hdr(buf->peek(), end - buf->peek());
        buf->retrieveUntil(end + 2);
        if (hdr.empty()) break;
    }
    buf->retrieveAll();  // 忽略 body

    if (path == "/" || path == "/dashboard" || path == "/index.html") {
        serveIndex(conn);
    } else if (path == "/api/dashboard/devices") {
        serveApiDevices(conn);
    } else if (path == "/api/dashboard/cluster") {
        serveApiCluster(conn);
    } else if (path == "/api/dashboard/alerts") {
        serveApiAlerts(conn);
    } else {
        serveNotFound(conn);
    }
}

// ============================================================
// 仪表盘首页
// ============================================================
void WebDashboard::serveIndex(const TcpConnectionPtr& conn) {
    sendHtml(conn, 200, buildDashboardHtml());
}

// ============================================================
// 设备列表 API
// ============================================================
void WebDashboard::serveApiDevices(const TcpConnectionPtr& conn) {
    size_t count = redis_ ? redis_->deviceCount() : 0;
    auto devices = redis_ ? redis_->getAllDevices() :
        std::vector<std::pair<std::string, std::string>>{};

    std::ostringstream oss;
    oss << R"({"code":0,"count":)" << count << R"(,"devices":[)";
    bool first = true;
    for (const auto& d : devices) {
        if (!first) oss << ",";
        oss << R"({"id":")" << d.first
            << R"(","gateway":")" << d.second
            << R"(","status":"online"})";
        first = false;
    }
    oss << "]}";

    sendHttpResponse(conn, 200, "application/json", oss.str());
}

// ============================================================
// 集群 API
// ============================================================
void WebDashboard::serveApiCluster(const TcpConnectionPtr& conn) {
    size_t deviceCount = redis_ ? redis_->deviceCount() : 0;
    int64_t pending = redis_ ? redis_->pendingCount() : 0;

    char buf[512];
    snprintf(buf, sizeof(buf),
        R"({"code":0,"cluster":{"devices":%zu,"pending_commands":%lld,"mode":"distributed"}})",
        deviceCount, (long long)pending);
    sendHttpResponse(conn, 200, "application/json", buf);
}

// ============================================================
// 告警 API
// ============================================================
void WebDashboard::serveApiAlerts(const TcpConnectionPtr& conn) {
    sendHttpResponse(conn, 200, "application/json",
        R"({"code":0,"alerts":[]})");
}

// ============================================================
// 404
// ============================================================
void WebDashboard::serveNotFound(const TcpConnectionPtr& conn) {
    sendHtml(conn, 404, "<h1>404 Not Found</h1>");
}

// ============================================================
// HTTP 响应工具
// ============================================================
void WebDashboard::sendHttpResponse(const TcpConnectionPtr& conn,
                                    int statusCode, const std::string& contentType,
                                    const std::string& body) {
    char header[512];
    const char* statusText = (statusCode == 200) ? "OK" :
                              (statusCode == 404) ? "Not Found" : "Error";

    int len = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n",
        statusCode, statusText, contentType.c_str(), body.size());

    conn->send(header, len);
    conn->send(body);
    conn->shutdown();
}

void WebDashboard::sendHtml(const TcpConnectionPtr& conn,
                            int statusCode, const std::string& html) {
    sendHttpResponse(conn, statusCode, "text/html; charset=utf-8", html);
}

// ============================================================
// 内嵌仪表盘 HTML
// ============================================================
std::string WebDashboard::buildDashboardHtml() const {
    return R"HTML(<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>分布式IoT设备监控与指令调度平台</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f1923; color: #e0e6ed; min-height: 100vh; }
.header { background: #1a2733; padding: 16px 32px; border-bottom: 1px solid #2a3a4a;
          display: flex; justify-content: space-between; align-items: center; }
.header h1 { font-size: 20px; color: #4fc3f7; }
.header .subtitle { font-size: 12px; color: #78909c; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 20px; padding: 24px; }
.card { background: #1a2733; border: 1px solid #2a3a4a; border-radius: 8px;
        padding: 20px; }
.card h3 { font-size: 14px; color: #78909c; margin-bottom: 12px;
           text-transform: uppercase; letter-spacing: 1px; }
.card .value { font-size: 36px; font-weight: 700; color: #4fc3f7; }
.card .label { font-size: 12px; color: #546e7a; margin-top: 4px; }
.status-ok { color: #66bb6a; }
.status-warn { color: #ffa726; }
.status-error { color: #ef5350; }
table { width: 100%; border-collapse: collapse; margin-top: 12px; }
th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #2a3a4a;
         font-size: 13px; }
th { color: #78909c; font-weight: 600; }
tr:hover { background: rgba(79,195,247,0.05); }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 11px; font-weight: 600; }
.badge-online { background: rgba(102,187,106,0.2); color: #66bb6a; }
.badge-distributed { background: rgba(79,195,247,0.2); color: #4fc3f7; }
.architecture { background: #0d1b24; border: 1px solid #2a3a4a; border-radius: 8px;
                padding: 24px; margin: 24px; font-family: monospace; font-size: 12px;
                line-height: 1.8; white-space: pre; overflow-x: auto; color: #78909c; }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>分布式IoT设备监控与指令调度平台</h1>
    <div class="subtitle">Distributed IoT Device Monitoring &amp; Command Dispatch Platform</div>
  </div>
  <div>
    <span class="badge badge-distributed">Distributed Mode</span>
    <span class="badge badge-online" style="margin-left:8px">Cluster Online</span>
  </div>
</div>

<div class="grid">
  <div class="card">
    <h3>在线设备总数</h3>
    <div class="value" id="deviceCount">--</div>
    <div class="label">分布式设备注册表 (Redis)</div>
  </div>
  <div class="card">
    <h3>集群节点</h3>
    <div class="value" id="nodeCount">--</div>
    <div class="label">Redis Streams 消费者组</div>
  </div>
  <div class="card">
    <h3>待处理指令</h3>
    <div class="value" id="pendingCmd">--</div>
    <div class="label">Redis Streams Pending</div>
  </div>
  <div class="card">
    <h3>系统状态</h3>
    <div class="value status-ok">RUNNING</div>
    <div class="label">Nginx L4 → Gateway Nodes → Redis</div>
  </div>
</div>

<div class="architecture">
                     ┌──────────────────────────┐
                     │    Nginx L4 负载均衡      │
                     │    listen :1883           │
                     │    stream proxy           │
                     └──────────┬───────────────┘
                ┌───────────────┼───────────────┐
                │               │               │
         ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
         │ Gateway-1   │ │ Gateway-2  │ │ Gateway-3  │
         │ :1883       │ │ :1884      │ │ :1885      │
         │ 10万设备     │ │ 10万设备    │ │ 10万设备    │
         └──────┬──────┘ └─────┬──────┘ └─────┬──────┘
                │               │               │
                └───────────────┼───────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
       ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
       │    Redis    │  │  InfluxDB   │  │  管理平台    │
       │ Streams     │  │  时序存储    │  │  Dashboard  │
       │ 设备注册表   │  │ 批量异步写入 │  │  :9090      │
       │ Pub/Sub路由  │  │             │  │             │
       └─────────────┘  └─────────────┘  └─────────────┘

  核心分布式特性:
  ✓ Redis Streams 消费者组 (XREADGROUP + XACK)
  ✓ 分布式设备注册表 (Redis Hash: iot:devices)
  ✓ 跨节点指令路由 (Redis Pub/Sub: iot:route:{nodeId})
  ✓ Nginx TCP 四层负载均衡 (stream proxy)
  ✓ 节点故障检测与心跳 (TTL-based Heartbeat)
  ✓ 水平扩展 (新节点加入自动负载均衡)
</div>

<div class="card" style="margin: 0 24px 24px 24px;">
  <h3>API 接口</h3>
  <table>
    <tr><th>Method</th><th>Path</th><th>说明</th></tr>
    <tr><td>POST</td><td>/api/v1/commands</td><td>下发指令 → Redis Streams (XADD)</td></tr>
    <tr><td>GET</td><td>/api/v1/devices</td><td>查询在线设备数 (Redis HLEN)</td></tr>
    <tr><td>GET</td><td>/api/v1/cluster</td><td>集群拓扑信息</td></tr>
    <tr><td>GET</td><td>/api/v1/health</td><td>健康检查</td></tr>
    <tr><td>GET</td><td>/api/v1/device/:id</td><td>设备详情 (Telemetry)</td></tr>
    <tr><td>GET</td><td>/api/v1/commands/:id</td><td>指令执行状态</td></tr>
  </table>
</div>

<script>
async function refresh() {
  try {
    let r = await fetch('/api/dashboard/devices'); let d = await r.json();
    document.getElementById('deviceCount').textContent = d.count || 0;

    r = await fetch('/api/dashboard/cluster'); let c = await r.json();
    document.getElementById('nodeCount').textContent = '3 (consumers)';
    document.getElementById('pendingCmd').textContent = c.cluster?.pending_commands || 0;
  } catch(e) { console.error(e); }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>)HTML";
}

std::string WebDashboard::buildDevicesHtml() const {
    return "<h1>Devices</h1>";
}

std::string WebDashboard::buildAlertsHtml() const {
    return "<h1>Alerts</h1>";
}
