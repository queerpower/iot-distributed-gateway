#pragma once
#include "common/NonCopyable.h"
#include <functional>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>

class EventLoop;

// ============================================================
// EventLoopThread — 将 EventLoop 绑定到独立线程
//
// 典型用法:
//   EventLoopThread th;
//   EventLoop* loop = th.startLoop();  // 阻塞直到线程就绪
//   loop->runInLoop(...);              // 安全提交任务
// ============================================================
class EventLoopThread : public NonCopyable {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThread(const ThreadInitCallback& cb = ThreadInitCallback(),
                    const std::string& name = std::string());
    ~EventLoopThread();

    // 启动线程, 返回线程中的 EventLoop 指针 (阻塞直到线程创建完 loop)
    EventLoop* startLoop();

private:
    void threadFunc();  // 线程入口

    EventLoop*          loop_;            // 线程内的 EventLoop (线程安全: mutex_ 保护)
    std::thread         thread_;
    std::mutex          mutex_;
    std::condition_variable cv_;
    ThreadInitCallback  initCallback_;
    std::string         name_;
};
