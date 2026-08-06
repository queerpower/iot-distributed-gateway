#pragma once
#include "common/NonCopyable.h"

// ============================================================
// RAII 套接字封装 — 自动关闭 fd, 禁止拷贝, 允许移动
// ============================================================
class Socket : public NonCopyable {
public:
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket();

    // 移动语义
    Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) { close(); fd_ = other.fd_; other.fd_ = -1; }
        return *this;
    }

    int fd() const { return fd_; }

    // ---- Socket 选项 ----
    void setReuseAddr(bool on);    // SO_REUSEADDR
    void setReusePort(bool on);    // SO_REUSEPORT
    void setTcpNoDelay(bool on);   // TCP_NODELAY (禁用 Nagle)
    void setKeepAlive(bool on);    // SO_KEEPALIVE

    // ---- 操作 ----
    void bind(const struct sockaddr_in& addr);
    void listen();
    int  accept(struct sockaddr_in* peerAddr);

    void shutdownWrite();          // 半关闭写端
    void close();                  // 强制关闭

    // ---- 静态工厂 ----
    static Socket createTcp(sa_family_t family = AF_INET);
    static Socket createUdp(sa_family_t family = AF_INET);

private:
    int fd_ = -1;
};
