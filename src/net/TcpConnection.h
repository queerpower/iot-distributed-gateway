#pragma once
#include "common/NonCopyable.h"
#include "Callbacks.h"
#include "Buffer.h"
#include "InetAddress.h"
#include <memory>
#include <string>
#include <atomic>

class EventLoop;
class Channel;
class Socket;

// ============================================================
// TcpConnection — 代表一条 TCP 连接
//
// 生命周期状态机:
//   kConnecting → kConnected → kDisconnecting → kDisconnected
//
// 线程安全模型:
//   - 所有 I/O 操作在所属 EventLoop 线程中执行
//   - send() / shutdown() 可跨线程调用 (通过 queueInLoop)
//   - 每个连接使用 shared_ptr 管理, 由 TcpServer 和 Channel 共同持有
//
// 关键设计:
//   - handleRead: 从 socket 读数据到 inputBuffer, 触发 messageCallback
//   - handleWrite: 把 outputBuffer 的数据发出去
//   - handleClose: 对端关闭或自身 error 后, 清理资源
//   - 发送队列: 如果要发送的数据放不进 outputBuffer (高水位),
//     则注册写事件, 等待 socket 可写时再继续发
// ============================================================
class TcpConnection : public NonCopyable,
                      public std::enable_shared_from_this<TcpConnection> {
public:
    enum StateE { kConnecting, kConnected, kDisconnecting, kDisconnected };

    TcpConnection(EventLoop* loop, const std::string& name,
                  int sockfd, const InetAddress& localAddr,
                  const InetAddress& peerAddr);
    ~TcpConnection();

    // ---- 只读 ----
    EventLoop*      getLoop() const     { return loop_; }
    const std::string& name() const     { return name_; }
    const InetAddress& localAddr() const { return localAddr_; }
    const InetAddress& peerAddr() const  { return peerAddr_; }
    bool connected() const              { return state_ == kConnected; }

    // ---- 回调设置 (由 TcpServer 调用) ----
    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb)       { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }
    void setHighWaterMarkCallback(const HighWaterMarkCallback& cb, size_t mark) {
        highWaterMarkCallback_ = cb; highWaterMark_ = mark;
    }
    void setCloseCallback(const CloseCallback& cb) { closeCallback_ = cb; }

    // ---- 连接生命周期 (由 TcpServer 调用) ----
    void connectEstablished();   // 连接建立后调用 (激活读事件)
    void connectDestroyed();     // 连接销毁前调用 (移除 Channel)

    // ---- 发送数据 (线程安全, 可在任意线程调用) ----
    void send(const std::string& msg);
    void send(const void* data, size_t len);
    void send(Buffer* buf);   // 交换 buffer 内容以提高效率

    // ---- 关闭连接 ----
    void shutdown();  // 优雅关闭 (半关闭写端)
    void forceClose();

    // ---- 上下文 (用于存储业务数据, 如设备 ID) ----
    void setContext(const std::string& ctx) { context_ = ctx; }
    const std::string& context() const { return context_; }

private:
    void handleRead(int64_t receiveTime);
    void handleWrite();
    void handleClose();
    void handleError();

    void sendInLoop(const std::string& msg);
    void sendInLoop(const void* data, size_t len);
    void shutdownInLoop();
    void forceCloseInLoop();

    void setState(StateE s) { state_ = s; }

    EventLoop*  loop_;
    std::string name_;
    std::atomic<StateE> state_;

    std::unique_ptr<Socket>  socket_;
    std::unique_ptr<Channel> channel_;
    InetAddress localAddr_;
    InetAddress peerAddr_;

    ConnectionCallback    connectionCallback_;
    MessageCallback       messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    HighWaterMarkCallback highWaterMarkCallback_;
    CloseCallback         closeCallback_;
    size_t                highWaterMark_;

    Buffer inputBuffer_;
    Buffer outputBuffer_;

    std::string context_;  // 业务上下文
};

// 用户可见的指针类型
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
