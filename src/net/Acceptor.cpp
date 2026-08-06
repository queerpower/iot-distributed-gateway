#include "Acceptor.h"
#include "EventLoop.h"
#include "Channel.h"
#include "common/Logger.h"
#include <fcntl.h>
#include <unistd.h>

// ============================================================
// 创建一个空的 /dev/null fd, 用于紧急情况:
// 当 accept 返回 EMFILE (进程 fd 耗尽) 时, 临时关闭此 fd,
// accept 一个新连接后立刻关闭它, 然后重新打开 /dev/null,
// 这样至少不会丢失所有新连接
// ============================================================
Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort)
    : loop_(loop)
    , acceptSocket_(Socket::createTcp())
    , acceptChannel_(loop, acceptSocket_.fd())
    , listening_(false)
    , idleFd_(::open("/dev/null", O_RDONLY | O_CLOEXEC)) {

    acceptSocket_.setReuseAddr(true);
    if (reusePort) {
        acceptSocket_.setReusePort(true);
    }
    acceptSocket_.bind(listenAddr.getSockAddr());

    acceptChannel_.setReadCallback([this](int64_t) { handleRead(); });
}

Acceptor::~Acceptor() {
    acceptChannel_.disableAll();
    acceptChannel_.remove();
    if (idleFd_ >= 0) {
        ::close(idleFd_);
    }
}

void Acceptor::listen() {
    loop_->assertInLoopThread();
    listening_ = true;
    acceptSocket_.listen();
    acceptChannel_.enableReading();  // 注册到 epoll
}

// ============================================================
// handleRead — accept 新连接, 分发给 TcpServer
// ============================================================
void Acceptor::handleRead() {
    loop_->assertInLoopThread();
    struct sockaddr_in peerAddr;
    int connFd = acceptSocket_.accept(&peerAddr);

    if (connFd >= 0) {
        if (newConnectionCallback_) {
            newConnectionCallback_(connFd, InetAddress(peerAddr));
        } else {
            ::close(connFd);  // 没有回调, 直接关闭
        }
    } else {
        // accept 出错处理
        if (errno == EMFILE) {
            // fd 耗尽: 关掉 idleFd_, accept 后立刻关闭, 再恢复 idleFd_
            ::close(idleFd_);
            idleFd_ = acceptSocket_.accept(&peerAddr);
            ::close(idleFd_);
            idleFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        }
    }
}
