#pragma once
#include "common/NonCopyable.h"
#include "Socket.h"
#include "InetAddress.h"
#include <functional>

class EventLoop;
class Channel;

// ============================================================
// Acceptor — 在 mainLoop 中监听端口, 接受新连接
//
// 工作流程:
//   1. 创建非阻塞 listening socket
//   2. 绑定地址 + listen
//   3. 注册到 EventLoop, 等待可读事件
//   4. 收到新连接 → accept → 回调 newConnectionCallback
// ============================================================
class Acceptor : public NonCopyable {
public:
    using NewConnectionCallback = std::function<void(int sockfd,
                                                     const InetAddress& peer)>;

    Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort = true);
    ~Acceptor();

    void setNewConnectionCallback(const NewConnectionCallback& cb) {
        newConnectionCallback_ = cb;
    }

    void listen();
    bool listening() const { return listening_; }

private:
    void handleRead();

    EventLoop* loop_;
    Socket     acceptSocket_;
    Channel    acceptChannel_;
    NewConnectionCallback newConnectionCallback_;
    bool       listening_;
    int        idleFd_;  // 预留 fd, 用于解决 accept 时 fd 耗尽的问题
};
