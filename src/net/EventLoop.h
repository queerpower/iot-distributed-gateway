#pragma once
#include "common/NonCopyable.h"
#include "common/Timestamp.h"
#include <functional>
#include <memory>
#include <vector>
#include <map>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>

class Channel;

// ============================================================
// EventLoop — Reactor 模型的核心
//
// 一个线程运行一个 EventLoop, 即 "One loop per thread"
//
// 核心职责:
//  1. 调用 epoll_wait 监听 fd 事件
//  2. 将就绪事件分发给对应的 Channel
//  3. 提供线程安全的 runInLoop/queueInLoop, 支持跨线程唤醒
//  4. 管理定时器
//
// 线程模型:
//  - EventLoop 对象必须在创建它的线程中调用 loop()
//  - 其他线程可通过 queueInLoop 把任务提交到 loop 线程执行
// ============================================================
class EventLoop : public NonCopyable {
public:
    using Functor = std::function<void()>;
    using TimerId = int64_t;

    EventLoop();
    ~EventLoop();

    // ---- 主循环 ----
    void loop();    // 阻塞运行, 直到 quit()
    void quit();    // 退出循环 (线程安全)

    // ---- 判断当前线程 ----
    bool isInLoopThread() const {
        return threadId_ == std::this_thread::get_id();
    }

    // ---- 线程安全的任务提交 ----
    // 在当前 loop 线程立即执行 (本线程) 或排队 (其他线程)
    void runInLoop(Functor cb);
    void queueInLoop(Functor cb);

    // ---- Channel 管理 (仅 loop 线程调用) ----
    void updateChannel(Channel* ch);
    void removeChannel(Channel* ch);
    bool hasChannel(Channel* ch);

    // ---- 定时器 ----
    // runAt:    指定时间点执行
    // runAfter: delay 秒后执行, 返回 TimerId 可取消
    // runEvery: 每 interval 秒重复执行, 返回 TimerId 可取消
    TimerId runAt(Timestamp when, Functor cb);
    TimerId runAfter(double delay, Functor cb);
    TimerId runEvery(double interval, Functor cb);
    // 取消定时器 (幂等)
    void cancelTimer(TimerId timerId);

    // ---- 唤醒 ----
    void wakeup();

    // ---- 调试 ----
    void assertInLoopThread() {
        if (!isInLoopThread()) { abortNotInLoopThread(); }
    }

    static EventLoop* eventLoopOfCurrentThread();

private:
    void abortNotInLoopThread();
    void handleWakeupRead();  // wakeupFd 可读回调
    void handleTimerRead();   // timerFd 可读回调
    void doPendingFunctors(); // 执行排队的任务

    // 定时器内部
    void resetTimerFd(Timestamp expiration);
    void processTimers();

    using ChannelList = std::vector<Channel*>;
    using TimerEntry = std::pair<Timestamp, Functor>;  // (过期时间, 回调)
    using TimerSet = std::multimap<int64_t, Functor>;  // 按微秒时间戳排序

    std::atomic<bool> looping_;
    std::atomic<bool> quit_;
    bool              callingPendingFunctors_;  // 防止 queueInLoop 递归膨胀

    std::thread::id   threadId_;
    int               epollFd_;
    int               wakeupFd_;     // eventfd, 用于线程间唤醒
    int               timerFd_;      // timerfd, 用于定时器

    std::unique_ptr<Channel> wakeupChannel_;
    std::unique_ptr<Channel> timerChannel_;

    ChannelList       activeChannels_;       // epoll_wait 返回的就绪 Channel
    Channel*          currentActiveChannel_; // 正在处理的那个

    std::vector<struct epoll_event> epollEvents_; // epoll_wait 事件数组

    std::vector<Functor> pendingFunctors_;   // 等待执行的任务队列
    std::mutex           mutex_;             // 保护 pendingFunctors_

    TimerSet            timers_;             // 定时器集合 (到期时间 → 回调)
    std::vector<Functor> expiredTimers_;     // 本轮到期的定时器回调

    // 定时器取消支持
    std::atomic<int64_t> nextTimerId_{1};
    std::mutex timerCancelMutex_;
    std::unordered_map<TimerId, std::shared_ptr<std::atomic<bool>>> timerCancelTokens_;

    static const int kPollTimeMs      = 10000;    // epoll_wait 超时 (ms)
    static const int kInitEventListSize = 16;      // 初始事件数组大小
};
