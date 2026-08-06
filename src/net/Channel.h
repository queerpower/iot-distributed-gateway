#pragma once
#include <functional>
#include <sys/epoll.h>

class EventLoop;

// ============================================================
// Channel — 每个 fd 对应一个 Channel, 管理其事件和回调
//
// 核心职责:
//  1. 封装 fd 及其关心的 I/O 事件 (可读/可写/错误/边缘触发等)
//  2. 持有事件就绪时的回调函数
//  3. 通过 EventLoop::updateChannel 把自己注册到 epoll
//
// 与 epoll 的关系:
//  - Channel 不拥有 fd (fd 由 Socket/Acceptor/Timer 拥有)
//  - Channel 是 fd 的 "事件视图", Channel 销毁 ≠ fd 关闭
// ============================================================
class Channel {
public:
    using EventCallback = std::function<void()>;
    using ReadEventCallback = std::function<void(int64_t)>;  // 参数 receiveTime

    Channel(EventLoop* loop, int fd);
    ~Channel();

    // ---- 事件处理 (由 EventLoop 调用) ----
    void handleEvent(int64_t receiveTime);

    // ---- 设置回调 ----
    void setReadCallback(ReadEventCallback cb)    { readCallback_  = std::move(cb); }
    void setWriteCallback(EventCallback cb)       { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb)       { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb)       { errorCallback_ = std::move(cb); }

    // ---- 事件开关 ----
    void enableReading()   { events_ |= kReadEvent;  update(); }
    void disableReading()  { events_ &= ~kReadEvent; update(); }
    void enableWriting()   { events_ |= kWriteEvent; update(); }
    void disableWriting()  { events_ &= ~kWriteEvent; update(); }
    void disableAll()      { events_ = kNoneEvent; update(); }
    bool isReading() const { return events_ & kReadEvent; }
    bool isWriting() const { return events_ & kWriteEvent; }
    bool isNoneEvent() const { return events_ == kNoneEvent; }
    bool hasEvents() const { return (events_ & (kReadEvent | kWriteEvent)) != 0; }

    // ---- 只读 ----
    int  fd() const          { return fd_; }
    int  events() const      { return events_; }
    void setRevents(int revt) { revents_ = revt; }  // epoll 回填

    EventLoop* ownerLoop() const { return loop_; }

    // ---- 生命周期标记 ----
    int  index() const         { return index_; }
    void setIndex(int idx)     { index_ = idx; }

    // ---- 移除自身 ----
    void remove();

    // ---- 绑定上下文 (TcpConnection 等) ----
    void setTie(const std::shared_ptr<void>& tie) { tie_ = tie; tied_ = true; }

private:
    void update();
    void handleEventWithGuard(int64_t receiveTime);

    static const int kNoneEvent  = 0;
    static const int kReadEvent  = EPOLLIN | EPOLLPRI;
    static const int kWriteEvent = EPOLLOUT;

    EventLoop*       loop_;
    const int        fd_;
    int              events_;    // 关心的 I/O 事件
    int              revents_;   // 已就绪的事件
    int              index_;     // epoll 中的状态 (-1=未注册, 1=已添加, 2=已删除)

    bool             tied_;       // 是否绑定了 shared_ptr 生命周期
    std::weak_ptr<void> tie_;    // 弱引用, 防止 handleEvent 时对象被析构

    ReadEventCallback readCallback_;
    EventCallback     writeCallback_;
    EventCallback     closeCallback_;
    EventCallback     errorCallback_;
};
