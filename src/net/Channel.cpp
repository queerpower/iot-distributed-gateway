#include "Channel.h"
#include "EventLoop.h"
#include "common/Logger.h"

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop)
    , fd_(fd)
    , events_(0)
    , revents_(0)
    , index_(-1)
    , tied_(false) {}

Channel::~Channel() {
    assert(!isReading() && !isWriting());
}

// ============================================================
// handleEvent — 事件分发
//
// 顺序: close > error > read > write
// 这样确保连接断开时优先处理清理逻辑
// ============================================================
void Channel::handleEvent(int64_t receiveTime) {
    if (tied_) {
        // 绑定生命周期: 如果 TcpConnection 还活着才处理事件
        std::shared_ptr<void> guard = tie_.lock();
        if (guard) {
            handleEventWithGuard(receiveTime);
        }
    } else {
        handleEventWithGuard(receiveTime);
    }
}

void Channel::handleEventWithGuard(int64_t receiveTime) {
    // POLLHUP: 对端关闭连接
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
        return;
    }

    // 错误事件
    if (revents_ & EPOLLERR) {
        if (errorCallback_) errorCallback_();
    }

    // 可读 (包括对端正常关闭 — 此时 read 返回 0)
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (readCallback_) readCallback_(receiveTime);
    }

    // 可写
    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
}

// ============================================================
// update — 把事件变更同步到 epoll
// ============================================================
void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::remove() {
    assert(isNoneEvent());
    loop_->removeChannel(this);
}
