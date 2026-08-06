#pragma once
#include "common/NonCopyable.h"
#include <functional>
#include <memory>
#include <vector>

class EventLoop;
class EventLoopThread;

// ============================================================
// EventLoopThreadPool — 多 Reactor 线程池
//
// 架构:
//  - 一个 mainLoop (用于 accept 新连接)
//  - N 个 subLoop (每个在独立线程中, 用于处理已建立连接的 I/O)
//  - 采用 round-robin (轮转) 策略分配连接
//
// 面试要点:
//  Q: Reactor 和 Proactor 的区别?
//  A: Reactor 是同步非阻塞 I/O, 由应用主动 read/write (本项目的模型);
//     Proactor 是异步 I/O, 内核完成 I/O 后通知应用 (如 IOCP).
//  Q: 为什么用 One loop per thread?
//  A: 每个线程一个 epoll 实例, 避免锁争用; 连接粘在固定线程上,
//     内存访问有缓存局部性.
// ============================================================
class EventLoopThreadPool : public NonCopyable {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThreadPool(EventLoop* baseLoop, const std::string& name);
    ~EventLoopThreadPool();

    // 设置线程数量 (必须在 start 之前调用)
    void setThreadNum(int num) { numThreads_ = num; }

    // 启动所有工作线程
    void start(const ThreadInitCallback& cb = ThreadInitCallback());

    // 轮转获取下一个 EventLoop (用于分配新连接)
    EventLoop* getNextLoop();

    // 获取所有 loops
    std::vector<EventLoop*> getAllLoops() const;

    bool started() const { return started_; }

private:
    EventLoop* baseLoop_;           // 主 loop (通常用于 accept)
    std::string name_;
    bool        started_;
    int         numThreads_;        // 工作线程数
    int         next_;              // round-robin 索引

    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;  // 每个线程的 EventLoop 指针
};
