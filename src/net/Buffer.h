#pragma once
#include <vector>
#include <cstddef>
#include <cassert>
#include <cstring>
#include <algorithm>

// ============================================================
// 网络缓冲区 — 仿 muduo 的 prependable 设计
//
// 内存布局:
// | prependable |  readable  |  writable  |
// |<-- readerIdx |<-- writerIdx |<-- capacity
//
// 设计要点:
//  1. 头部预留空间 (kCheapPrepend), 方便添加协议头而不搬数据
//  2. 自动扩容, vector 管理内存
//  3. readFd() 使用 readv 配合栈缓冲区减少系统调用
// ============================================================
class Buffer {
public:
    static const size_t kCheapPrepend = 8;   // 预留头部
    static const size_t kInitialSize  = 1024; // 初始容量

    explicit Buffer(size_t initSize = kInitialSize)
        : buffer_(kCheapPrepend + initSize)
        , readerIndex_(kCheapPrepend)
        , writerIndex_(kCheapPrepend) {}

    // ---- 可读/可写空间查询 ----
    size_t readableBytes() const { return writerIndex_ - readerIndex_; }
    size_t writableBytes() const { return buffer_.size() - writerIndex_; }
    size_t prependableBytes() const { return readerIndex_; }

    const char* peek() const { return begin() + readerIndex_; }

    // ---- 数据读取 (从缓冲区取走数据) ----
    void retrieve(size_t len) {
        assert(len <= readableBytes());
        if (len < readableBytes()) {
            readerIndex_ += len;
        } else {
            retrieveAll();
        }
    }

    void retrieveAll() {
        readerIndex_ = kCheapPrepend;
        writerIndex_ = kCheapPrepend;
    }

    // 取出全部可读数据为字符串
    std::string retrieveAllAsString() {
        std::string result(peek(), readableBytes());
        retrieveAll();
        return result;
    }

    // 消费数据直到指定位置 (不包含 end 指向的字节)
    void retrieveUntil(const char* end) {
        assert(end >= peek());
        assert(end <= beginWrite());
        retrieve(end - peek());
    }

    std::string retrieveAsString(size_t len) {
        assert(len <= readableBytes());
        std::string result(peek(), len);
        retrieve(len);
        return result;
    }

    // ---- 数据写入 (往缓冲区添加数据) ----
    void append(const char* data, size_t len) {
        ensureWritableBytes(len);
        std::copy(data, data + len, beginWrite());
        hasWritten(len);
    }

    void append(const void* data, size_t len) {
        append(static_cast<const char*>(data), len);
    }

    void append(const std::string& str) {
        append(str.data(), str.size());
    }

    // 头部插入 (如添加消息长度前缀)
    void prepend(const void* data, size_t len) {
        assert(len <= prependableBytes());
        readerIndex_ -= len;
        const char* d = static_cast<const char*>(data);
        std::copy(d, d + len, begin() + readerIndex_);
    }

    // ---- 从 fd 读取 (核心方法) ----
    ssize_t readFd(int fd, int* savedErrno);

    // ---- 查找换行符 (用于行协议) ----
    const char* findCRLF() const {
        const char* start = peek();
        const char* end = start + readableBytes();
        const char* crlf = std::search(start, end, kCRLF, kCRLF + 2);
        return crlf == end ? nullptr : crlf;
    }

    // ---- 扩容 ----
    void ensureWritableBytes(size_t len) {
        if (writableBytes() < len) {
            makeSpace(len);
        }
    }

    size_t internalCapacity() const { return buffer_.capacity(); }

private:
    char* begin() { return buffer_.data(); }
    const char* begin() const { return buffer_.data(); }
    char* beginWrite() { return begin() + writerIndex_; }
    const char* beginWrite() const { return begin() + writerIndex_; }

    void hasWritten(size_t len) {
        assert(len <= writableBytes());
        writerIndex_ += len;
    }

    // 扩容策略: 优先整理空间 (挪动数据到头部), 不够才扩容
    void makeSpace(size_t len);

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writerIndex_;

    static const char kCRLF[];
};
