#pragma once
#include <functional>
#include <memory>
#include "common/Timestamp.h"

// ============================================================
// 回调类型定义 — 整个网络层共享的回调别名
// ============================================================

class TcpConnection;
class Buffer;

// 连接建立/关闭
using ConnectionCallback  = std::function<void(const std::shared_ptr<TcpConnection>&)>;
// 收到消息 (数据在 Buffer 中)
using MessageCallback     = std::function<void(const std::shared_ptr<TcpConnection>&,
                                               Buffer*, Timestamp)>;
// 写完成
using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
// 高水位线回调 (发送缓冲区积压过多)
using HighWaterMarkCallback = std::function<void(const std::shared_ptr<TcpConnection>&,
                                                  size_t)>;
// 关闭回调
using CloseCallback        = std::function<void(const std::shared_ptr<TcpConnection>&)>;
