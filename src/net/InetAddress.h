#pragma once
#include <netinet/in.h>
#include <string>

// ============================================================
// 封装 IPv4 套接字地址 (sockaddr_in)
// ============================================================
class InetAddress {
public:
    // 通配地址 + 端口 (用于监听)
    explicit InetAddress(uint16_t port, bool loopbackOnly = false);

    // 指定 IP + 端口
    InetAddress(const std::string& ip, uint16_t port);

    // 从已有 sockaddr_in 构造
    explicit InetAddress(const struct sockaddr_in& addr)
        : addr_(addr) {}

    // ---- 只读 ----
    const struct sockaddr_in& getSockAddr() const { return addr_; }
    void setSockAddr(const struct sockaddr_in& addr) { addr_ = addr; }

    std::string toIp() const;
    uint16_t    toPort() const;
    std::string toIpPort() const;

private:
    struct sockaddr_in addr_;
};
