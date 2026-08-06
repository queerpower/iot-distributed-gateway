#pragma once
#include "common/NonCopyable.h"
#include "Callbacks.h"
#include "TcpConnection.h"
#include "InetAddress.h"
#include <memory>
#include <string>
#include <map>
#include <atomic>

class EventLoop;
class Acceptor;
class EventLoopThreadPool;

// ============================================================
// TcpServer — TCP 服务器门面 (Facade)
//
// 使用方式:
//   EventLoop loop;
//   TcpServer server(&loop, InetAddress(1883), "Gateway");
//   server.setConnectionCallback(...);
//   server.setMessageCallback(...);
//   server.setThreadNum(4);  // 4 个 worker 线程
//   server.start();
//   loop.loop();
//
// 内部协调:
//   Acceptor (mainLoop) → 新连接 → EventLoopThreadPool (round-robin)
//   → TcpConnection (绑定到 worker loop)
// ============================================================
class TcpServer : public NonCopyable {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    TcpServer(EventLoop* loop, const InetAddress& listenAddr,
              const std::string& name, bool reusePort = true);
    ~TcpServer();

    // ---- 配置 (在 start 之前设置) ----
    void setThreadNum(int num);
    void setThreadInitCallback(const ThreadInitCallback& cb) {
        threadInitCallback_ = cb;
    }
    void setConnectionCallback(const ConnectionCallback& cb) {
        connectionCallback_ = cb;
    }
    void setMessageCallback(const MessageCallback& cb) {
        messageCallback_ = cb;
    }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) {
        writeCompleteCallback_ = cb;
    }

    // ---- 启动 ----
    void start();

    // ---- 只读 ----
    EventLoop* getLoop() const { return loop_; }
    const std::string& name() const { return name_; }
    const std::string& ipPort() const { return ipPort_; }

private:
    void newConnection(int sockfd, const InetAddress& peerAddr);
    void removeConnection(const TcpConnectionPtr& conn);
    void removeConnectionInLoop(const TcpConnectionPtr& conn);

    using ConnectionMap = std::map<std::string, TcpConnectionPtr>;

    EventLoop*    loop_;          // mainLoop (accept 所在线程)
    const std::string ipPort_;
    const std::string name_;

    std::unique_ptr<Acceptor> acceptor_;                // 只存在于 mainLoop
    std::unique_ptr<EventLoopThreadPool> threadPool_;   // worker 线程池

    ConnectionCallback    connectionCallback_;
    MessageCallback       messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    ThreadInitCallback    threadInitCallback_;

    std::atomic<int> started_;
    int               nextConnId_;      // 连接 ID 自增计数器
    ConnectionMap     connections_;     // name → TcpConnectionPtr (mainLoop 访问)
};
