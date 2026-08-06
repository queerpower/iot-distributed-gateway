// ============================================================
// echo_test.cpp — Echo 服务器测试
//
// 用于验证网络库基础功能:
//   1. 单线程 Reactor 模型
//   2. TCP 连接管理
//   3. 数据收发
//
// 使用方法:
//   ./echo_test
//   然后在另一个终端: telnet 127.0.0.1 7777
//   输入任意文本, 服务器会原样返回
// ============================================================

#include "net/EventLoop.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"
#include "net/InetAddress.h"
#include "common/Logger.h"
#include <cstdio>

class EchoServer {
public:
    EchoServer(EventLoop* loop, uint16_t port)
        : server_(loop, InetAddress(port), "EchoServer") {

        server_.setConnectionCallback(
            [this](const TcpConnectionPtr& conn) {
                if (conn->connected()) {
                    LOG_INFO("Echo: new connection from %s",
                             conn->peerAddr().toIpPort().c_str());
                } else {
                    LOG_INFO("Echo: connection %s closed",
                             conn->name().c_str());
                }
            });

        server_.setMessageCallback(
            [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
                // Echo: 将收到的数据原样返回
                std::string msg = buf->retrieveAllAsString();
                LOG_DEBUG("Echo: received %zu bytes from %s",
                          msg.size(), conn->name().c_str());

                conn->send(msg);

                // 如果收到 "quit", 关闭连接
                if (msg.find("quit") != std::string::npos) {
                    conn->shutdown();
                }
            });
    }

    void setThreadNum(int num) { server_.setThreadNum(num); }
    void start() { server_.start(); }

private:
    TcpServer server_;
};

int main() {
    Logger::setLevel(Logger::TRACE);  // 全量日志, 便于调试

    LOG_INFO("========================================");
    LOG_INFO("Echo Server Test");
    LOG_INFO("Listen on port 7777");
    LOG_INFO("Connect: telnet 127.0.0.1 7777");
    LOG_INFO("========================================");

    EventLoop loop;
    EchoServer echo(&loop, 7777);

    // 可以测试多线程模式:
    // echo.setThreadNum(4);  // 4 个 worker 线程

    echo.start();

    // 5 秒后打印统计 (示例)
    loop.runEvery(5.0, []() {
        LOG_INFO("Echo server is still running...");
    });

    loop.loop();
    return 0;
}
