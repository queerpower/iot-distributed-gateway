#include "ApiServer.h"
#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include "common/Logger.h"
#include <sstream>
#include <cstring>

ApiServer::ApiServer(EventLoop* loop, uint16_t port)
    : loop_(loop), port_(port) {
    auto addr = InetAddress(port);
    tcpServer_ = std::make_unique<TcpServer>(loop, addr, "HTTP-API");
}

ApiServer::~ApiServer() = default;

void ApiServer::setThreadNum(int num) { tcpServer_->setThreadNum(num); }

void ApiServer::start() {
    tcpServer_->setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { onConnection(conn); });
    tcpServer_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            onMessage(conn, buf, ts);
        });
    tcpServer_->start();
    LOG_INFO("ApiServer started on port %u", port_);
}

void ApiServer::stop() {}

void ApiServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        LOG_DEBUG("HTTP API: connection from %s", conn->peerAddr().toIpPort().c_str());
    }
}

void ApiServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
    handleHttpRequest(conn, buf);
}

// ============================================================
// 简易 HTTP 请求解析
// ============================================================
void ApiServer::handleHttpRequest(const TcpConnectionPtr& conn, Buffer* buf) {
    const char* crlf = buf->findCRLF();
    if (!crlf) return;

    std::string requestLine(buf->peek(), crlf - buf->peek());
    buf->retrieveUntil(crlf + 2);

    std::istringstream iss(requestLine);
    std::string method, path, version;
    iss >> method >> path >> version;

    // 跳过 HTTP 头部
    while (true) {
        const char* lineEnd = buf->findCRLF();
        if (!lineEnd) return;
        std::string line(buf->peek(), lineEnd - buf->peek());
        buf->retrieveUntil(lineEnd + 2);
        if (line.empty()) break;
    }

    std::string body(buf->peek(), buf->readableBytes());
    buf->retrieveAll();

    LOG_DEBUG("HTTP %s %s", method.c_str(), path.c_str());

    // ---- 路由分发 ----
    if (method == "POST" && path == "/api/v1/commands") {
        handlePostCommand(conn, body);
        return;
    }
    if (method == "GET" && path == "/api/v1/devices") {
        handleGetDevices(conn);
        return;
    }
    if (method == "GET" && path == "/api/v1/stats") {
        handleGetStats(conn);
        return;
    }
    if (method == "GET" && path == "/api/v1/cluster") {
        handleGetCluster(conn);
        return;
    }
    if (method == "GET" && path == "/api/v1/nodes") {
        handleGetNodes(conn);
        return;
    }
    if (method == "GET" && path == "/api/v1/health") {
        handleGetHealth(conn);
        return;
    }
    // /api/v1/device/:id
    if (method == "GET" && path.find("/api/v1/device/") == 0) {
        std::string deviceId = path.substr(15);
        handleGetDeviceDetail(conn, deviceId);
        return;
    }
    // /api/v1/commands/:id
    if (method == "GET" && path.find("/api/v1/commands/") == 0) {
        std::string cmdIdStr = path.substr(18);
        handleGetCommandStatus(conn, std::stoll(cmdIdStr));
        return;
    }

    handleNotFound(conn);
}

// ============================================================
// POST /api/v1/commands — 下发指令
// ============================================================
void ApiServer::handlePostCommand(const TcpConnectionPtr& conn,
                                  const std::string& body) {
    auto extractJsonStr = [&body](const std::string& key) -> std::string {
        std::string search = "\"" + key + "\"";
        auto pos = body.find(search);
        if (pos == std::string::npos) return "";
        pos = body.find('"', pos + search.size());
        if (pos == std::string::npos) return "";
        auto end = body.find('"', pos + 1);
        if (end == std::string::npos) return "";
        return body.substr(pos + 1, end - pos - 1);
    };

    std::string deviceId = extractJsonStr("device_id");
    std::string command  = extractJsonStr("command");
    std::string params   = extractJsonStr("params");
    if (params.empty()) params = "{}";

    if (deviceId.empty() || command.empty()) {
        sendJsonResponse(conn, 400,
            R"({"code":-1,"message":"missing device_id or command"})");
        return;
    }

    int64_t cmdId = 0;
    if (commandSenderFn_) {
        cmdId = commandSenderFn_(deviceId, command, params);
    }

    char buf[256];
    snprintf(buf, sizeof(buf),
        R"({"code":0,"command_id":%lld,"device_id":"%s","status":"enqueued","message":"ok"})",
        (long long)cmdId, deviceId.c_str());
    sendJsonResponse(conn, 200, buf);

    LOG_INFO("API: command [%lld] enqueued for device [%s]: %s",
             (long long)cmdId, deviceId.c_str(), command.c_str());
}

// ============================================================
// GET /api/v1/devices — 在线设备数
// ============================================================
void ApiServer::handleGetDevices(const TcpConnectionPtr& conn) {
    size_t count = deviceCountFn_ ? deviceCountFn_() : 0;
    char buf[128];
    snprintf(buf, sizeof(buf),
        R"({"code":0,"online_devices":%zu})", count);
    sendJsonResponse(conn, 200, buf);
}

// ============================================================
// GET /api/v1/device/:id — 设备详情
// ============================================================
void ApiServer::handleGetDeviceDetail(const TcpConnectionPtr& conn,
                                      const std::string& deviceId) {
    if (deviceDetailFn_) {
        std::string detail = deviceDetailFn_(deviceId);
        sendJsonResponse(conn, 200, detail);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf),
            R"({"code":0,"device":{"id":"%s","status":"unknown"}})",
            deviceId.c_str());
        sendJsonResponse(conn, 200, buf);
    }
}

// ============================================================
// GET /api/v1/commands/:id — 指令状态
// ============================================================
void ApiServer::handleGetCommandStatus(const TcpConnectionPtr& conn,
                                       int64_t commandId) {
    if (commandStatusFn_) {
        std::string status = commandStatusFn_(commandId);
        char buf[256];
        snprintf(buf, sizeof(buf),
            R"({"code":0,"command_id":%lld,"status":"%s"})",
            (long long)commandId, status.c_str());
        sendJsonResponse(conn, 200, buf);
    } else {
        char buf[128];
        snprintf(buf, sizeof(buf),
            R"({"code":0,"command_id":%lld,"status":"unknown"})",
            (long long)commandId);
        sendJsonResponse(conn, 200, buf);
    }
}

// ============================================================
// GET /api/v1/stats — 网关统计
// ============================================================
void ApiServer::handleGetStats(const TcpConnectionPtr& conn) {
    size_t devices = deviceCountFn_ ? deviceCountFn_() : 0;
    char buf[256];
    snprintf(buf, sizeof(buf),
        R"({"code":0,"stats":{"online_devices":%zu,"uptime_seconds":0}})",
        devices);
    sendJsonResponse(conn, 200, buf);
}

// ============================================================
// GET /api/v1/cluster — 集群统计 (分布式核心 API)
// ============================================================
void ApiServer::handleGetCluster(const TcpConnectionPtr& conn) {
    if (clusterStatsFn_) {
        sendJsonResponse(conn, 200,
            "{\"code\":0,\"cluster\":" + clusterStatsFn_() + "}");
    } else {
        sendJsonResponse(conn, 200,
            R"({"code":0,"cluster":{"mode":"standalone"}})");
    }
}

// ============================================================
// GET /api/v1/nodes — 集群节点列表
// ============================================================
void ApiServer::handleGetNodes(const TcpConnectionPtr& conn) {
    // 简化实现: 含在 cluster stats 中
    if (clusterStatsFn_) {
        sendJsonResponse(conn, 200,
            "{\"code\":0,\"nodes\":" + clusterStatsFn_() + "}");
    } else {
        sendJsonResponse(conn, 200,
            R"({"code":0,"nodes":[{"id":"standalone","status":"online"}]})");
    }
}

// ============================================================
// GET /api/v1/health — 健康检查
// ============================================================
void ApiServer::handleGetHealth(const TcpConnectionPtr& conn) {
    sendJsonResponse(conn, 200,
        R"({"status":"ok","service":"iot-gateway","version":"2.0.0"})");
}

// ============================================================
// 404
// ============================================================
void ApiServer::handleNotFound(const TcpConnectionPtr& conn) {
    sendJsonResponse(conn, 404,
        R"({"code":-1,"message":"not found"})");
}

// ============================================================
// HTTP 响应工具
// ============================================================
void ApiServer::sendHttpResponse(const TcpConnectionPtr& conn,
                                 int statusCode,
                                 const std::string& contentType,
                                 const std::string& body) {
    char header[512];
    const char* statusText = (statusCode == 200) ? "OK" :
                              (statusCode == 400) ? "Bad Request" :
                              (statusCode == 404) ? "Not Found" : "Error";

    int headerLen = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        statusCode, statusText, contentType.c_str(), body.size());

    conn->send(header, headerLen);
    conn->send(body);
    conn->shutdown();
}

void ApiServer::sendJsonResponse(const TcpConnectionPtr& conn,
                                int statusCode, const std::string& json) {
    sendHttpResponse(conn, statusCode, "application/json", json);
}
