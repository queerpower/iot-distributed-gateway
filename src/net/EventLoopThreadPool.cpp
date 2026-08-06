#include "EventLoopThreadPool.h"
#include "EventLoop.h"
#include "EventLoopThread.h"
#include "common/Logger.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop,
                                         const std::string& name)
    : baseLoop_(baseLoop)
    , name_(name)
    , started_(false)
    , numThreads_(0)
    , next_(0) {}

EventLoopThreadPool::~EventLoopThreadPool() {
    // unique_ptr 自动析构, 线程会 join
}

void EventLoopThreadPool::start(const ThreadInitCallback& cb) {
    assert(!started_);
    baseLoop_->assertInLoopThread();
    started_ = true;

    for (int i = 0; i < numThreads_; ++i) {
        char buf[64];
        snprintf(buf, sizeof(buf), "%s-sub-%d", name_.c_str(), i);

        auto* thread = new EventLoopThread(cb, buf);
        threads_.emplace_back(thread);
        loops_.push_back(thread->startLoop());
    }

    // 如果没有设置工作线程数 (numThreads_ == 0),
    // 则所有连接都在 baseLoop_ 处理 (单 Reactor)
    if (numThreads_ == 0 && cb) {
        cb(baseLoop_);
    }
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    baseLoop_->assertInLoopThread();

    if (loops_.empty()) {
        return baseLoop_;  // 单 Reactor 模式
    }

    // Round-robin 轮转
    EventLoop* loop = loops_[next_];
    ++next_;
    if (static_cast<size_t>(next_) >= loops_.size()) {
        next_ = 0;
    }
    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops() const {
    if (loops_.empty()) {
        return { baseLoop_ };
    }
    return loops_;
}
