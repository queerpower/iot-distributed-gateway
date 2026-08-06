#include "EventLoopThread.h"
#include "EventLoop.h"
#include "common/Logger.h"

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb,
                                 const std::string& name)
    : loop_(nullptr)
    , initCallback_(cb)
    , name_(name) {}

EventLoopThread::~EventLoopThread() {
    if (loop_ && thread_.joinable()) {
        loop_->quit();         // 通知 loop 退出
        thread_.join();        // 等待线程退出
    }
}

EventLoop* EventLoopThread::startLoop() {
    // 创建一个线程, 执行 threadFunc
    thread_ = std::thread(&EventLoopThread::threadFunc, this);

    // 阻塞等待: 直到线程内部的 EventLoop 创建完成
    EventLoop* result = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (loop_ == nullptr) {
            cv_.wait(lock);
        }
        result = loop_;
    }
    return result;
}

void EventLoopThread::threadFunc() {
    // 在新线程栈上创建 EventLoop
    EventLoop loop;

    if (initCallback_) {
        initCallback_(&loop);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;  // 发布给 startLoop 调用者
    }
    cv_.notify_one();

    // 进入事件循环 (阻塞)
    loop.loop();

    // loop 退出后的清理
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}
