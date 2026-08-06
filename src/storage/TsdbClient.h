#pragma once
#include "common/NonCopyable.h"
#include <string>
#include <functional>

class EventLoop;

// ============================================================
// TSDB 配置 (InfluxDB 兼容)
// ============================================================
struct TsdbConfig {
    std::string host = "127.0.0.1";
    int         port = 8086;
    std::string database = "iot_devices";
    int         writeTimeout = 5;  // 秒
};

// ============================================================
// TsdbClient — 时序数据库客户端 (InfluxDB Line Protocol)
//
// 核心职责:
//  1. 异步写入设备遥测数据 (非阻塞)
//  2. 使用 InfluxDB Line Protocol 格式
//
// 写入格式 (Line Protocol):
//   <measurement>,<tag_key>=<tag_value> <field_key>=<field_value> <timestamp>
//
// 示例:
//   device_data,device_id=sensor001 topic=/temp value=23.5 1620000000000000000
//
// 设计决策 — 为什么用异步写入:
//  网关核心链路不能因为数据库慢而阻塞设备数据接收,
//  使用队列 + 批量写入模式, 保证核心路径的低延迟
// ============================================================
class TsdbClient : public NonCopyable {
public:
    TsdbClient(EventLoop* loop, const TsdbConfig& cfg);
    ~TsdbClient();

    bool connect();
    void disconnect();

    // 异步写入设备数据 (将数据放入批量队列, 由定时器批量发送)
    void writeDeviceData(const std::string& deviceId,
                         const std::string& measurement,  // topic 作为 measurement
                         const std::string& value);       // payload

    // 手动刷新批量缓冲区
    void flush();

    bool isConnected() const { return connected_; }

private:
    void sendHttpPost(const std::string& body);  // HTTP POST 到 InfluxDB

    EventLoop*  loop_;
    TsdbConfig  config_;
    bool        connected_;

    // 批量写入缓冲区
    std::string batchBuffer_;
    int         batchCount_ = 0;
    static const int kMaxBatchSize = 100;    // 攒满 100 条或超时后批量写入
    static const int kMaxBatchBytes = 1024 * 1024;  // 最大 1MB
};
