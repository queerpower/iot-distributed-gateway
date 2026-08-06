#include "Socket.h"
#include "common/Logger.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <cstring>

Socket::~Socket() { close(); }

// ============================================================
// 创建非阻塞 TCP/UDP socket
// ============================================================
Socket Socket::createTcp(sa_family_t family) {
    int fd = ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("Socket::createTcp failed: %s", strerror(errno));
        abort();
    }
    return Socket(fd);
}

Socket Socket::createUdp(sa_family_t family) {
    int fd = ::socket(family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("Socket::createUdp failed: %s", strerror(errno));
        abort();
    }
    return Socket(fd);
}

// ============================================================
// Socket 选项
// ============================================================
void Socket::setReuseAddr(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

void Socket::setReusePort(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
}

void Socket::setTcpNoDelay(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

void Socket::setKeepAlive(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
}

// ============================================================
// 基本操作
// ============================================================
void Socket::bind(const struct sockaddr_in& addr) {
    int ret = ::bind(fd_, reinterpret_cast<const struct sockaddr*>(&addr),
                     sizeof(addr));
    if (ret < 0) {
        LOG_ERROR("Socket::bind failed: %s", strerror(errno));
        abort();
    }
}

void Socket::listen() {
    int ret = ::listen(fd_, SOMAXCONN);
    if (ret < 0) {
        LOG_ERROR("Socket::listen failed: %s", strerror(errno));
        abort();
    }
}

int Socket::accept(struct sockaddr_in* peerAddr) {
    socklen_t addrLen = sizeof(*peerAddr);
    int connFd = ::accept4(fd_, reinterpret_cast<struct sockaddr*>(peerAddr),
                           &addrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connFd < 0) {
        int savedErr = errno;
        if (savedErr == EAGAIN || savedErr == EWOULDBLOCK || savedErr == EINTR) {
            return -1;  // 暂时没有新连接, 正常情况
        }
        LOG_ERROR("Socket::accept failed: %s", strerror(savedErr));
    }
    return connFd;
}

void Socket::shutdownWrite() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_WR);
    }
}

void Socket::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}
