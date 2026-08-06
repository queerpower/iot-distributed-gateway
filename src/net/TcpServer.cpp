#include "TcpServer.h"
#include "Acceptor.h"
#include "EventLoop.h"
#include "EventLoopThreadPool.h"
#include "TcpConnection.h"
#include "common/Logger.h"

TcpServer::TcpServer(EventLoop* loop, const InetAddress& listenAddr,
                     const std::string& name, bool reusePort)
    : loop_(loop)
    , ipPort_(listenAddr.toIpPort())
    , name_(name)
    , started_(0)
    , nextConnId_(1) {

    acceptor_ = std::make_unique<Acceptor>(loop, listenAddr, reusePort);
    acceptor_->setNewConnectionCallback(
        [this](int sockfd, const InetAddress& peer) {
            newConnection(sockfd, peer);
        });
}

TcpServer::~TcpServer() {
    loop_->assertInLoopThread();
    for (auto& [name, conn] : connections_) {
        conn->forceClose();
    }
}

// ============================================================
// 配置工作线程数
// ============================================================
void TcpServer::setThreadNum(int num) {
    assert(num >= 0);
    threadPool_ = std::make_unique<EventLoopThreadPool>(loop_, name_);
    threadPool_->setThreadNum(num);
}

// ============================================================
// start — 启动线程池, 开始监听
// ============================================================
void TcpServer::start() {
    if (started_++ == 0) {
        if (threadPool_) {
            threadPool_->start(threadInitCallback_);
        }
        assert(!acceptor_->listening());
        loop_->runInLoop([this] { acceptor_->listen(); });
    }
}

// ============================================================
// newConnection — acceptor 收到新连接后的回调
//
// 流程:
//  1. 从线程池轮转选一个 EventLoop
//  2. 创建 TcpConnection
//  3. 在选中的 loop 中调用 connectEstablished
//  4. 注册到 connections_ 表
// ============================================================
void TcpServer::newConnection(int sockfd, const InetAddress& peerAddr) {
    loop_->assertInLoopThread();

    // 选一个 worker loop
    EventLoop* ioLoop = threadPool_ ? threadPool_->getNextLoop() : loop_;

    // 生成连接名
    char buf[64];
    snprintf(buf, sizeof(buf), "%s#%d", ipPort_.c_str(), nextConnId_++);
    std::string connName = buf;

    // 获取本端地址
    struct sockaddr_in localAddr;
    socklen_t addrLen = sizeof(localAddr);
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr),
                      &addrLen) < 0) {
        LOG_ERROR("getsockname failed");
    }

    // 创建 TcpConnection
    TcpConnectionPtr conn = std::make_shared<TcpConnection>(
        ioLoop, connName, sockfd, InetAddress(localAddr), peerAddr);

    connections_[connName] = conn;

    // 设置回调
    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setWriteCompleteCallback(writeCompleteCallback_);
    conn->setCloseCallback(
        [this](const TcpConnectionPtr& c) { removeConnection(c); });

    // 在 ioLoop 线程中完成连接建立
    ioLoop->runInLoop([conn] { conn->connectEstablished(); });
}

// ============================================================
// removeConnection — 从连接表中移除 (跨线程 → mainLoop)
// ============================================================
void TcpServer::removeConnection(const TcpConnectionPtr& conn) {
    loop_->runInLoop([this, conn] { removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn) {
    loop_->assertInLoopThread();
    size_t n = connections_.erase(conn->name());
    if (n != 1) {
        LOG_WARN("removeConnection: %s not found", conn->name().c_str());
        return;
    }

    // 在 conn 所属的 ioLoop 中销毁
    EventLoop* ioLoop = conn->getLoop();
    ioLoop->queueInLoop([conn] { conn->connectDestroyed(); });
}
