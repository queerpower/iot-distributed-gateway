#pragma once
#include "common/NonCopyable.h"
#include <string>
#include <vector>
#include <functional>
#include <unordered_map>
#include <atomic>
#include <mutex>

class EventLoop;

// ============================================================
// 告警级别
// ============================================================
enum class AlertLevel {
    INFO,
    WARN,
    CRITICAL,
    FATAL
};

// ============================================================
// 告警规则
// ============================================================
struct AlertRule {
    std::string name;               // 规则名称
    std::string metric;             // 监控指标 (device_offline, cpu_high, etc.)
    std::string condition;          // 条件表达式 (>90, <10, ==0)
    double      threshold = 0;      // 阈值
    AlertLevel  level = AlertLevel::WARN;
    int         cooldownSec = 300;  // 冷却时间 (秒), 避免重复告警
    bool        enabled = true;
};

// ============================================================
// 告警事件
// ============================================================
struct AlertEvent {
    int64_t     id;
    std::string ruleName;
    std::string metric;
    AlertLevel  level;
    std::string message;
    std::string detail;             // JSON 详细信息
    int64_t     timestamp;          // 微秒
    bool        resolved = false;
    int64_t     resolvedAt = 0;
};

// ============================================================
// AlertEngine — 告警引擎
//
// 功能:
//  1. 定义告警规则 (metric + threshold + condition)
//  2. 定时评估规则 (每 evaluationIntervalSec 检查)
//  3. 触发告警事件 (带冷却机制, 防抖)
//  4. 告警通知 (日志 / HTTP 回调 / Webhook)
//  5. 告警历史记录 (内存环形缓冲)
//
// 内置规则示例:
//  - device_offline_rate > 5% → WARN
//  - node_count < min_nodes → CRITICAL
//  - pending_commands > 1000 → WARN
//  - redis_disconnected → FATAL
// ============================================================
class AlertEngine : public NonCopyable {
public:
    using AlertCallback = std::function<void(const AlertEvent&)>;
    using MetricProvider = std::function<double()>;

    AlertEngine(EventLoop* loop);
    ~AlertEngine();

    // ---- 规则管理 ----
    void addRule(const AlertRule& rule);
    void removeRule(const std::string& name);
    void enableRule(const std::string& name, bool enabled);
    std::vector<AlertRule> getRules() const;

    // ---- 指标注册 ----
    void registerMetric(const std::string& name, MetricProvider provider);

    // ---- 生命周期 ----
    void start(double evaluationIntervalSec = 10.0);
    void stop();

    // ---- 告警回调 ----
    void setAlertCallback(AlertCallback cb) { alertCb_ = std::move(cb); }

    // ---- 历史查询 ----
    std::vector<AlertEvent> getRecentAlerts(int limit = 50) const;
    size_t activeAlertCount() const;

private:
    void evaluate();
    void fireAlert(const AlertRule& rule, double currentValue,
                  const std::string& detail);
    bool isInCooldown(const std::string& ruleName, int cooldownSec) const;

    EventLoop* loop_;
    std::atomic<bool> running_{false};

    // 规则
    mutable std::mutex rulesMutex_;
    std::vector<AlertRule> rules_;

    // 指标
    std::unordered_map<std::string, MetricProvider> metrics_;

    // 告警历史 (环形缓冲)
    mutable std::mutex alertsMutex_;
    std::vector<AlertEvent> alertHistory_;
    static const size_t kMaxHistorySize = 1000;

    // 冷却追踪: ruleName → lastFireTime
    mutable std::mutex cooldownMutex_;
    std::unordered_map<std::string, int64_t> cooldowns_;

    AlertCallback alertCb_;
    int64_t evaluationTimerId_ = 0;
    std::atomic<int64_t> nextAlertId_{1};
};
