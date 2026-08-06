#include "EventLoop.h"
#include "Channel.h"
#include "common/Logger.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <sstream>

// ============================================================
// thread_local 存储: 每个线程拥有自己的 EventLoop 指针
// ============================================================
static thread_local EventLoop* t_loopInThisThread = nullptr;

EventLoop* EventLoop::eventLoopOfCurrentThread() {
    return t_loopInThisThread;
}

// ============================================================
// 构造: 创建 epoll fd, wakeup fd, timer fd
// ============================================================
EventLoop::EventLoop()
    : looping_(false)
    , quit_(false)
    , callingPendingFunctors_(false)
    , threadId_(std::this_thread::get_id())
    , currentActiveChannel_(nullptr)
    , epollEvents_(kInitEventListSize) {

    // 断言: 一个线程只能有一个 EventLoop
    if (t_loopInThisThread) {
        LOG_ERROR("Another EventLoop %p exists in this thread %lu",
                  t_loopInThisThread, threadId_);
        abort();
    }
    t_loopInThisThread = this;

    // ---- 创建 epoll 实例 ----
    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ < 0) {
        LOG_ERROR("epoll_create1 failed: %s", strerror(errno));
        abort();
    }

    // ---- 创建 eventfd (用于线程间唤醒) ----
    wakeupFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeupFd_ < 0) {
        LOG_ERROR("eventfd failed: %s", strerror(errno));
        abort();
    }
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback([this](int64_t) { handleWakeupRead(); });
    wakeupChannel_->enableReading();

    // ---- 创建 timerfd (用于定时器) ----
    timerFd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerFd_ < 0) {
        LOG_ERROR("timerfd_create failed: %s", strerror(errno));
        abort();
    }
    timerChannel_ = std::make_unique<Channel>(this, timerFd_);
    timerChannel_->setReadCallback([this](int64_t) { handleTimerRead(); });
    timerChannel_->enableReading();
}

EventLoop::~EventLoop() {
    // 移除所有 Channel
    wakeupChannel_->disableAll();
    wakeupChannel_->remove();
    timerChannel_->disableAll();
    timerChannel_->remove();
    ::close(wakeupFd_);
    ::close(timerFd_);
    ::close(epollFd_);
    t_loopInThisThread = nullptr;
}

// ============================================================
// loop() — 事件循环主函数
//
// 伪代码:
// while (!quit) {
//     activeChannels = epoll_wait(timeout);
//     for each channel in activeChannels:
//         channel.handleEvent();
//     doPendingFunctors();
// }
// ============================================================
void EventLoop::loop() {
    assertInLoopThread();
    looping_ = true;
    quit_ = false;
    LOG_INFO("EventLoop %p starts looping", this);

    while (!quit_) {
        activeChannels_.clear();

        // 阻塞等待 I/O 事件 (或超时)
        int numEvents = ::epoll_wait(epollFd_,
                                     epollEvents_.data(),
                                     static_cast<int>(epollEvents_.size()),
                                     kPollTimeMs);
        int savedErrno = errno;

        if (numEvents > 0) {
            LOG_TRACE("%d events happened", numEvents);

            // 扩容处理: 如果事件数达到上限, 下次扩容一倍
            if (static_cast<size_t>(numEvents) == epollEvents_.size()) {
                epollEvents_.resize(epollEvents_.size() * 2);
            }

            // 先把就绪 Channel 收集起来
            for (int i = 0; i < numEvents; ++i) {
                Channel* ch = static_cast<Channel*>(epollEvents_[i].data.ptr);
                ch->setRevents(epollEvents_[i].events);
                activeChannels_.push_back(ch);
            }

            // 分发事件
            for (Channel* ch : activeChannels_) {
                currentActiveChannel_ = ch;
                ch->handleEvent(Timestamp::now().microSecondsSinceEpoch());
            }
            currentActiveChannel_ = nullptr;
        } else if (numEvents == 0) {
            // 超时 — 正常, 可用于定时器检查
        } else {
            if (savedErrno != EINTR) {
                LOG_ERROR("epoll_wait error: %s", strerror(savedErrno));
            }
        }

        // 执行排队的任务
        doPendingFunctors();
    }

    LOG_INFO("EventLoop %p stops looping", this);
    looping_ = false;
}

// ============================================================
// quit — 安全退出 (可跨线程调用)
// ============================================================
void EventLoop::quit() {
    quit_ = true;
    if (!isInLoopThread()) {
        wakeup();  // 让阻塞在 epoll_wait 的线程醒来
    }
}

// ============================================================
// runInLoop / queueInLoop — 线程安全的闭包执行
// ============================================================
void EventLoop::runInLoop(Functor cb) {
    if (isInLoopThread()) {
        cb();
    } else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor cb) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    // 如果是其他线程调用, 必须唤醒 loop 线程
    // 如果正在执行 pendingFunctors_ 但又有新任务入队, 也需要唤醒
    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
}

// ============================================================
// doPendingFunctors — 执行队列中的闭包
// ============================================================
void EventLoop::doPendingFunctors() {
    std::vector<Functor> functors;
    callingPendingFunctors_ = true;

    {
        // 缩小锁粒度: 只交换, 不加锁执行
        std::lock_guard<std::mutex> lock(mutex_);
        functors.swap(pendingFunctors_);
    }

    for (const auto& f : functors) {
        f();
    }

    callingPendingFunctors_ = false;
}

// ============================================================
// wakeup — 向 eventfd 写 8 字节, 唤醒 epoll_wait
// ============================================================
void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR("EventLoop::wakeup write error");
    }
}

void EventLoop::handleWakeupRead() {
    uint64_t val = 0;
    ssize_t n = ::read(wakeupFd_, &val, sizeof(val));
    if (n != sizeof(val)) {
        LOG_ERROR("EventLoop::handleWakeupRead error");
    }
}

void EventLoop::handleTimerRead() {
    uint64_t expirations = 0;
    ssize_t n = ::read(timerFd_, &expirations, sizeof(expirations));
    if (n != sizeof(expirations)) {
        LOG_ERROR("EventLoop::handleTimerRead error");
        return;
    }
    processTimers();
}

// ============================================================
// Channel 管理 — 更新/移除 Channel 与 epoll 的关联
// ============================================================
void EventLoop::updateChannel(Channel* ch) {
    assertInLoopThread();
    const int idx = ch->index();

    struct epoll_event ev;
    ev.events   = ch->events();
    ev.data.ptr = ch;

    if (idx == -1) {
        // 新 Channel, 添加到 epoll
        int ret = ::epoll_ctl(epollFd_, EPOLL_CTL_ADD, ch->fd(), &ev);
        if (ret < 0) {
            LOG_ERROR("epoll_ctl ADD fd=%d failed: %s", ch->fd(), strerror(errno));
        } else {
            ch->setIndex(1);  // 标记为已添加
        }
    } else {
        // 已有 Channel, 修改关注的事件
        int ret = ::epoll_ctl(epollFd_, EPOLL_CTL_MOD, ch->fd(), &ev);
        if (ret < 0) {
            LOG_ERROR("epoll_ctl MOD fd=%d failed: %s", ch->fd(), strerror(errno));
        }
    }
}

void EventLoop::removeChannel(Channel* ch) {
    assertInLoopThread();
    assert(ch->isNoneEvent());

    struct epoll_event ev;
    int ret = ::epoll_ctl(epollFd_, EPOLL_CTL_DEL, ch->fd(), &ev);
    if (ret < 0) {
        LOG_ERROR("epoll_ctl DEL fd=%d failed: %s", ch->fd(), strerror(errno));
    }
    ch->setIndex(-1);
}

bool EventLoop::hasChannel(Channel* ch) {
    assertInLoopThread();
    return ch->ownerLoop() == this;
}

// ============================================================
// 定时器实现 (v2.0: 支持取消)
// ============================================================
EventLoop::TimerId EventLoop::runAt(Timestamp when, Functor cb) {
    assertInLoopThread();
    TimerId id = nextTimerId_++;
    // 用 cancel token 包裹回调
    auto token = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard<std::mutex> lock(timerCancelMutex_);
        timerCancelTokens_[id] = token;
    }
    auto wrapped = [this, id, cb = std::move(cb), token]() {
        if (!token->load()) {
            cb();
        }
        // 清理 token (单次定时器)
        std::lock_guard<std::mutex> lock(timerCancelMutex_);
        timerCancelTokens_.erase(id);
    };
    timers_.emplace(when.microSecondsSinceEpoch(), std::move(wrapped));
    if (timers_.begin()->first == when.microSecondsSinceEpoch()) {
        resetTimerFd(when);
    }
    return id;
}

EventLoop::TimerId EventLoop::runAfter(double delay, Functor cb) {
    Timestamp when = Timestamp::now() + delay;
    return runAt(when, std::move(cb));
}

EventLoop::TimerId EventLoop::runEvery(double interval, Functor cb) {
    assertInLoopThread();
    TimerId id = nextTimerId_++;
    // 取消 token: 设为 true 则下次触发时不再重新调度
    auto token = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard<std::mutex> lock(timerCancelMutex_);
        timerCancelTokens_[id] = token;
    }
    // 使用 shared_ptr 包装, 递归调度
    auto timerCb = std::make_shared<Functor>();
    *timerCb = [this, interval, cb = std::move(cb), timerCb, token, id]() {
        if (!token->load()) {
            cb();
        }
        if (!token->load()) {
            Timestamp next = Timestamp::now() + interval;
            timers_.emplace(next.microSecondsSinceEpoch(), *timerCb);
            if (timers_.begin()->first == next.microSecondsSinceEpoch()) {
                resetTimerFd(next);
            }
        } else {
            // 被取消, 清理 token
            // token 的 mutex 清理延迟到下次 processTimers 之后
        }
    };
    Timestamp first = Timestamp::now() + interval;
    timers_.emplace(first.microSecondsSinceEpoch(), *timerCb);
    if (timers_.begin()->first == first.microSecondsSinceEpoch()) {
        resetTimerFd(first);
    }
    return id;
}

void EventLoop::cancelTimer(TimerId timerId) {
    // 线程安全: 可在任意线程调用
    std::lock_guard<std::mutex> lock(timerCancelMutex_);
    auto it = timerCancelTokens_.find(timerId);
    if (it != timerCancelTokens_.end()) {
        it->second->store(true);
        timerCancelTokens_.erase(it);
    }
}

void EventLoop::resetTimerFd(Timestamp expiration) {
    struct itimerspec newValue;
    struct itimerspec oldValue;
    std::memset(&newValue, 0, sizeof(newValue));
    std::memset(&oldValue, 0, sizeof(oldValue));

    int64_t microSec = expiration.microSecondsSinceEpoch()
                       - Timestamp::now().microSecondsSinceEpoch();
    if (microSec < 100) {
        microSec = 100;  // 最小 100 微秒
    }

    newValue.it_value.tv_sec = static_cast<time_t>(microSec / 1000000);
    newValue.it_value.tv_nsec = static_cast<long>((microSec % 1000000) * 1000);

    int ret = ::timerfd_settime(timerFd_, 0, &newValue, &oldValue);
    if (ret < 0) {
        LOG_ERROR("timerfd_settime failed: %s", strerror(errno));
    }
}

void EventLoop::processTimers() {
    assertInLoopThread();
    Timestamp now = Timestamp::now();

    // 收集到期的定时器
    for (auto it = timers_.begin(); it != timers_.end();) {
        if (it->first <= now.microSecondsSinceEpoch()) {
            expiredTimers_.push_back(it->second);
            it = timers_.erase(it);
        } else {
            break;  // 后面都是未到期的
        }
    }

    // 执行到期的回调
    for (const auto& cb : expiredTimers_) {
        cb();
    }
    expiredTimers_.clear();

    // 重新设置 timerfd 为下一个最近的定时器
    if (!timers_.empty()) {
        resetTimerFd(Timestamp(timers_.begin()->first));
    }
}

void EventLoop::abortNotInLoopThread() {
    std::ostringstream oss;
    oss << "EventLoop::abortNotInLoopThread - current thread: "
        << std::this_thread::get_id()
        << " is not the loop thread: " << threadId_;
    LOG_ERROR("%s", oss.str().c_str());
    abort();
}
