#include "Buffer.h"
#include <sys/uio.h>
#include <errno.h>

const char Buffer::kCRLF[] = "\r\n";

// ============================================================
// readFd — 使用 readv 和栈缓冲区减少系统调用
//
// 为什么用 readv + 栈缓冲?
//   - readv 可以一次系统调用读到两块内存
//   - 如果 Buffer 的 writable 空间不够, 先用栈缓冲接住, 再 append
//   - 避免 read() 返回 EAGAIN 时什么也没读到
// ============================================================
ssize_t Buffer::readFd(int fd, int* savedErrno) {
    char extrabuf[65536];  // 64K 栈缓冲
    struct iovec vec[2];
    const size_t writable = writableBytes();

    vec[0].iov_base = beginWrite();
    vec[0].iov_len  = writable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len  = sizeof(extrabuf);

    const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
    ssize_t n = ::readv(fd, vec, iovcnt);
    if (n < 0) {
        *savedErrno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        writerIndex_ += n;
    } else {
        // 数据量大于 writable 空间 — 溢出的部分在 extrabuf 中
        writerIndex_ = buffer_.size();
        append(extrabuf, n - writable);
    }
    return n;
}

// ============================================================
// makeSpace — 自动扩容
//
// 策略:
//  1. 如果前置空闲 + 尾部空闲足够 → 整理碎片 (把可读数据往前挪)
//  2. 否则 → 扩容 (至少翻倍, 或扩展到正好够用)
// ============================================================
void Buffer::makeSpace(size_t len) {
    if (writableBytes() + prependableBytes() < len + kCheapPrepend) {
        // 真扩容
        buffer_.resize(writerIndex_ + len);
    } else {
        // 整理碎片 — 把 [readerIndex_, writerIndex_) 移到 kCheapPrepend 开始处
        size_t readable = readableBytes();
        std::copy(begin() + readerIndex_,
                  begin() + writerIndex_,
                  begin() + kCheapPrepend);
        readerIndex_ = kCheapPrepend;
        writerIndex_ = readerIndex_ + readable;
    }
}
