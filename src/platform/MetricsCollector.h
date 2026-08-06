#pragma once
#include "common/NonCopyable.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <cstdint>

class EventLoop;

// ============================================================
// 指标采样点
// ============================================================
struct MetricSample {
    std::string name;
    double      value;
    int64_t     timestamp;  // 微秒
    std::unordered_map<std::string, std::string> tags;
};

// ============================================================
// 聚合指标
// ============================================================
struct AggregatedMetric {
    std::string name;
    double      min = 0;
    double      max = 0;
    double      avg = 0;
    double      p50 = 0;
    double      p95 = 0;
    double      p99 = 0;
    int64_t     sampleCount = 0;
    int64_t     windowStart = 0;  // 微秒
    int64_t     windowEnd = 0;
};

// ============================================================
// MetricsCollector — 指标采集器
//
// 采集指标:
//  - gateway.connections.active   — 活跃连接数
//  - gateway.connections.total    — 累计连接数
//  - gateway.messages.received    — 接收消息数
//  - gateway.messages.sent        — 发送消息数
//  - gateway.commands.consumed    — 消费指令数
//  - gateway.commands.forwarded   — 转发指令数
//  - gateway.commands.failed      — 失败指令数
//  - gateway.commands.pending     — 待处理指令数
//  - cluster.nodes.online         — 在线节点数
//  - cluster.devices.total        — 总设备数
//  - redis.connected              — Redis 连接状态
//  - system.cpu.percent           — CPU 使用率
//  - system.memory.used_mb        — 内存使用
//
// 聚合窗口:
//  - 最近 1 分钟 (60s)
//  - 最近 5 分钟 (300s)
//  - 最近 15 分钟 (900s)
// ============================================================
class MetricsCollector : public NonCopyable {
public:
    MetricsCollector(EventLoop* loop);
    ~MetricsCollector();

    // ---- 指标记录 ----
    void record(const std::string& name, double value,
               const std::unordered_map<std::string, std::string>& tags = {});
    void increment(const std::string& name, double delta = 1.0);
    void setGauge(const std::string& name, double value);

    // ---- 查询 ----
    // 获取最近一次值
    double getLatest(const std::string& name) const;
    // 获取聚合值 (最近 windowSec 秒)
    AggregatedMetric getAggregated(const std::string& name,
                                   int windowSec = 60) const;
    // 获取所有指标名称
    std::vector<std::string> getMetricNames() const;
    // 导出为 JSON (供 API 查询)
    std::string toJson() const;

    // ---- 生命周期 ----
    void start(int collectIntervalSec = 10);
    void stop();

private:
    void pruneOldSamples();

    EventLoop* loop_;
    std::atomic<bool> running_{false};
    int64_t collectTimerId_ = 0;

    // name → samples
    mutable std::mutex mutex_;
    struct SampleData {
        std::vector<MetricSample> samples;
        double latestValue = 0;
        double counterValue = 0;  // 累计值 (用于 increment)
    };
    std::unordered_map<std::string, SampleData> data_;

    static const size_t kMaxSamplesPerMetric = 2000;  // 约 5.5 小时 (10s间隔)
    static const int    kDefaultWindowSec = 60;
};
