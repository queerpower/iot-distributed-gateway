#include "TcpConnection.h"
#include "EventLoop.h"
#include "Channel.h"
#include "Socket.h"
#include "common/Logger.h"
#include <unistd.h>

TcpConnection::TcpConnection(EventLoop* loop, const std::string& name,
                             int sockfd, const InetAddress& localAddr,
                             const InetAddress& peerAddr)
    : loop_(loop)
    , name_(name)
    , state_(kConnecting)
    , socket_(new Socket(sockfd))
    , channel_(new Channel(loop, sockfd))
    , localAddr_(localAddr)
    , peerAddr_(peerAddr)
    , highWaterMark_(64 * 1024 * 1024) {  // 默认 64MB 高水位

    // 设置 Channel 回调
    channel_->setReadCallback(
        [this](int64_t receiveTime) { handleRead(receiveTime); });
    channel_->setWriteCallback(
        [this]() { handleWrite(); });
    channel_->setCloseCallback(
        [this]() { handleClose(); });
    channel_->setErrorCallback(
        [this]() { handleError(); });

    LOG_DEBUG("TcpConnection[%s] fd=%d created", name_.c_str(), sockfd);
}

TcpConnection::~TcpConnection() {
    LOG_DEBUG("TcpConnection[%s] fd=%d destroyed", name_.c_str(),
              channel_ ? channel_->fd() : -1);
}

// ============================================================
// connectEstablished — 连接建立后, 启用读事件
// ============================================================
void TcpConnection::connectEstablished() {
    loop_->assertInLoopThread();
    assert(state_ == kConnecting);
    setState(kConnected);

    // 绑定生命周期: Channel 持有 TcpConnection 的弱引用,
    // 防止 handleEvent 时对象已析构
    channel_->setTie(shared_from_this());
    channel_->enableReading();

    // 触发用户回调
    if (connectionCallback_) {
        connectionCallback_(shared_from_this());
    }
}

// ============================================================
// connectDestroyed — 连接销毁, 清理资源
// ============================================================
void TcpConnection::connectDestroyed() {
    loop_->assertInLoopThread();
    if (state_ == kConnected) {
        setState(kDisconnected);
        channel_->disableAll();
        if (connectionCallback_) {
            connectionCallback_(shared_from_this());
        }
    }
    channel_->remove();
}

// ============================================================
// handleRead — 读到数据, 触发 messageCallback
//
// 关键点: 一次 read 可能读到多条消息 (TCP 是字节流)
//         应用层协议 (MQTT) 需要做分包处理
//         这里只负责读, 分包逻辑在上层 (协议层/网关层)
// ============================================================
void TcpConnection::handleRead(int64_t receiveTime) {
    loop_->assertInLoopThread();
    int savedErrno = 0;
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &savedErrno);

    if (n > 0) {
        if (messageCallback_) {
            messageCallback_(shared_from_this(), &inputBuffer_,
                             Timestamp(receiveTime));
        }
    } else if (n == 0) {
        // 对端正常关闭 (FIN)
        handleClose();
    } else {
        // 读出错
        errno = savedErrno;
        LOG_ERROR("TcpConnection::handleRead error: %s", strerror(savedErrno));
        handleError();
    }
}

// ============================================================
// handleWrite — socket 可写, 继续发送 outputBuffer_ 中的数据
//
// 为什么要注册写事件?
//   直接 write() 可能只发了一部分数据 (非阻塞 socket),
//   剩余数据暂存在 outputBuffer_ 中, 等 socket 可写时再发
// ============================================================
void TcpConnection::handleWrite() {
    loop_->assertInLoopThread();
    if (!channel_->isWriting()) return;

    ssize_t n = ::write(channel_->fd(),
                        outputBuffer_.peek(),
                        outputBuffer_.readableBytes());
    if (n > 0) {
        outputBuffer_.retrieve(n);
        if (outputBuffer_.readableBytes() == 0) {
            // 全部发送完毕, 取消写事件监听
            channel_->disableWriting();
            if (writeCompleteCallback_) {
                loop_->queueInLoop(
                    [conn = shared_from_this()] {
                        conn->writeCompleteCallback_(conn);
                    });
            }
            if (state_ == kDisconnecting) {
                shutdownInLoop();  // 关闭写端
            }
        }
    } else {
        LOG_ERROR("TcpConnection::handleWrite error");
    }
}

// ============================================================
// handleClose — 对端关闭连接
// ============================================================
void TcpConnection::handleClose() {
    loop_->assertInLoopThread();
    assert(state_ == kConnected || state_ == kDisconnecting);
    setState(kDisconnected);
    channel_->disableAll();

    TcpConnectionPtr guard = shared_from_this();
    if (connectionCallback_) connectionCallback_(guard);
    if (closeCallback_) closeCallback_(guard);
}

// ============================================================
// handleError — 连接错误处理
// ============================================================
void TcpConnection::handleError() {
    int err;
    socklen_t len = sizeof(err);
    if (::getsockopt(channel_->fd(), SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        err = errno;
    }
    LOG_ERROR("TcpConnection::handleError [%s] - SO_ERROR = %s",
              name_.c_str(), strerror(err));
}

// ============================================================
// send — 线程安全的数据发送
//
// 如果当前在 loop 线程 → 直接 send
// 如果不在 → queueInLoop (跨线程安全)
// ============================================================
void TcpConnection::send(const std::string& msg) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(msg.data(), msg.size());
        } else {
            loop_->runInLoop(
                [conn = shared_from_this(), msg] {
                    conn->sendInLoop(msg.data(), msg.size());
                });
        }
    }
}

void TcpConnection::send(const void* data, size_t len) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(data, len);
        } else {
            std::string msg(static_cast<const char*>(data), len);
            loop_->runInLoop(
                [conn = shared_from_this(), msg] {
                    conn->sendInLoop(msg.data(), msg.size());
                });
        }
    }
}

void TcpConnection::send(Buffer* buf) {
    if (state_ != kConnected) return;
    if (loop_->isInLoopThread()) {
        sendInLoop(buf->peek(), buf->readableBytes());
        buf->retrieveAll();
    } else {
        std::string msg = buf->retrieveAllAsString();
        loop_->runInLoop(
            [conn = shared_from_this(), msg] {
                conn->sendInLoop(msg.data(), msg.size());
            });
    }
}

// ============================================================
// sendInLoop — 在 loop 线程中实际发送数据
//
// 发送策略:
//  1. 如果 outputBuffer_ 为空 → 直接 write (减少一次内存拷贝)
//  2. 如果没发完 → 把剩余数据 append 到 outputBuffer_,
//     注册写事件, 等 epoll 通知可写时继续发
//  3. 如果 outputBuffer_ 超过高水位 → 触发高水位回调 (限流)
// ============================================================
void TcpConnection::sendInLoop(const void* data, size_t len) {
    loop_->assertInLoopThread();
    ssize_t n = 0;
    size_t remaining = len;
    bool fault = false;

    if (state_ == kDisconnected) {
        LOG_WARN("TcpConnection::send abandoned, connection closed");
        return;
    }

    // 如果没有待发数据, 尝试直接 write
    if (!channel_->isWriting() && outputBuffer_.readableBytes() == 0) {
        n = ::write(channel_->fd(), data, len);
        if (n >= 0) {
            remaining = len - n;
            if (remaining == 0 && writeCompleteCallback_) {
                // 全部发出, 通知写完成
                loop_->queueInLoop(
                    [conn = shared_from_this()] {
                        conn->writeCompleteCallback_(conn);
                    });
            }
        } else {
            n = 0;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LOG_ERROR("TcpConnection::sendInLoop write error");
                if (errno == EPIPE || errno == ECONNRESET) {
                    fault = true;
                }
            }
        }
    }

    assert(remaining <= len);
    if (!fault && remaining > 0) {
        // 数据没发完, 缓存到 outputBuffer_
        size_t oldLen = outputBuffer_.readableBytes();
        if (oldLen + remaining >= highWaterMark_
            && oldLen < highWaterMark_
            && highWaterMarkCallback_) {
            loop_->queueInLoop(
                [conn = shared_from_this(), total = oldLen + remaining] {
                    conn->highWaterMarkCallback_(conn, total);
                });
        }
        outputBuffer_.append(static_cast<const char*>(data) + n, remaining);
        if (!channel_->isWriting()) {
            channel_->enableWriting();  // 注册写事件
        }
    }
}

// ============================================================
// shutdown / forceClose
// ============================================================
void TcpConnection::shutdown() {
    if (state_ == kConnected) {
        setState(kDisconnecting);
        loop_->runInLoop([conn = shared_from_this()] { conn->shutdownInLoop(); });
    }
}

void TcpConnection::shutdownInLoop() {
    loop_->assertInLoopThread();
    if (!channel_->isWriting()) {
        socket_->shutdownWrite();  // 半关闭: 发 FIN
    }
    // 如果还有数据在发送中, 等 handleWrite 完成后再关闭
}

void TcpConnection::forceClose() {
    if (state_ == kConnected || state_ == kDisconnecting) {
        setState(kDisconnecting);
        loop_->queueInLoop([conn = shared_from_this()] { conn->forceCloseInLoop(); });
    }
}

void TcpConnection::forceCloseInLoop() {
    loop_->assertInLoopThread();
    if (state_ == kConnected || state_ == kDisconnecting) {
        handleClose();
    }
}
