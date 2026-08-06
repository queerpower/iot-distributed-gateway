#include "AlertEngine.h"
#include "net/EventLoop.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <algorithm>
#include <cmath>

AlertEngine::AlertEngine(EventLoop* loop) : loop_(loop) {
    // 添加内置规则
    addRule({"device_offline_rate", "gateway.devices.offline",
             ">", 5.0, AlertLevel::WARN, 300, true});
    addRule({"high_pending_commands", "gateway.commands.pending",
             ">", 1000.0, AlertLevel::WARN, 120, true});
    addRule({"node_count_low", "cluster.nodes.online",
             "<", 1.0, AlertLevel::CRITICAL, 60, true});
    addRule({"redis_disconnected", "redis.connected",
             "==", 0.0, AlertLevel::FATAL, 30, true});
}

AlertEngine::~AlertEngine() { stop(); }

void AlertEngine::addRule(const AlertRule& rule) {
    std::lock_guard<std::mutex> lock(rulesMutex_);
    rules_.push_back(rule);
    LOG_INFO("AlertEngine: rule [%s] added (metric=%s, threshold=%.1f)",
             rule.name.c_str(), rule.metric.c_str(), rule.threshold);
}

void AlertEngine::removeRule(const std::string& name) {
    std::lock_guard<std::mutex> lock(rulesMutex_);
    rules_.erase(std::remove_if(rules_.begin(), rules_.end(),
        [&](const AlertRule& r) { return r.name == name; }),
        rules_.end());
}

void AlertEngine::enableRule(const std::string& name, bool enabled) {
    std::lock_guard<std::mutex> lock(rulesMutex_);
    for (auto& r : rules_) {
        if (r.name == name) { r.enabled = enabled; break; }
    }
}

std::vector<AlertRule> AlertEngine::getRules() const {
    std::lock_guard<std::mutex> lock(rulesMutex_);
    return rules_;
}

void AlertEngine::registerMetric(const std::string& name, MetricProvider provider) {
    metrics_[name] = std::move(provider);
}

void AlertEngine::start(double intervalSec) {
    running_ = true;
    evaluationTimerId_ = loop_->runEvery(intervalSec, [this]() { evaluate(); });
    LOG_INFO("AlertEngine: started (interval=%.1fs)", intervalSec);
}

void AlertEngine::stop() {
    running_ = false;
    if (evaluationTimerId_ > 0) {
        loop_->cancelTimer(evaluationTimerId_);
        evaluationTimerId_ = 0;
    }
}

// ============================================================
// evaluate — 评估所有规则
// ============================================================
void AlertEngine::evaluate() {
    std::lock_guard<std::mutex> lock(rulesMutex_);
    for (const auto& rule : rules_) {
        if (!rule.enabled) continue;
        if (isInCooldown(rule.name, rule.cooldownSec)) continue;

        auto it = metrics_.find(rule.metric);
        if (it == metrics_.end()) continue;

        double value = it->second();
        bool triggered = false;

        if (rule.condition == ">") {
            triggered = (value > rule.threshold);
        } else if (rule.condition == "<") {
            triggered = (value < rule.threshold);
        } else if (rule.condition == "==") {
            triggered = (std::abs(value - rule.threshold) < 0.001);
        } else if (rule.condition == ">=") {
            triggered = (value >= rule.threshold);
        }

        if (triggered) {
            char detail[256];
            snprintf(detail, sizeof(detail),
                R"({"metric":"%s","value":%.2f,"threshold":%.2f})",
                rule.metric.c_str(), value, rule.threshold);
            fireAlert(rule, value, detail);
        }
    }
}

void AlertEngine::fireAlert(const AlertRule& rule, double currentValue,
                           const std::string& detail) {
    AlertEvent event;
    event.id = nextAlertId_++;
    event.ruleName = rule.name;
    event.metric = rule.metric;
    event.level = rule.level;
    event.timestamp = Timestamp::now().microSecondsSinceEpoch();

    const char* levelStr = "INFO";
    switch (rule.level) {
        case AlertLevel::WARN:     levelStr = "WARN"; break;
        case AlertLevel::CRITICAL: levelStr = "CRITICAL"; break;
        case AlertLevel::FATAL:    levelStr = "FATAL"; break;
        default: break;
    }

    char msg[256];
    snprintf(msg, sizeof(msg),
        "[%s] %s: metric=%s value=%.2f threshold=%.1f",
        levelStr, rule.name.c_str(), rule.metric.c_str(),
        currentValue, rule.threshold);
    event.message = msg;
    event.detail = detail;

    // 记录冷却
    {
        std::lock_guard<std::mutex> lock(cooldownMutex_);
        cooldowns_[rule.name] = event.timestamp;
    }

    // 保存到历史
    {
        std::lock_guard<std::mutex> lock(alertsMutex_);
        if (alertHistory_.size() >= kMaxHistorySize) {
            alertHistory_.erase(alertHistory_.begin());
        }
        alertHistory_.push_back(event);
    }

    // 日志
    switch (rule.level) {
        case AlertLevel::INFO:     LOG_INFO("%s", msg); break;
        case AlertLevel::WARN:     LOG_WARN("%s", msg); break;
        case AlertLevel::CRITICAL: LOG_ERROR("%s", msg); break;
        case AlertLevel::FATAL:    LOG_ERROR("%s", msg); break;
    }

    // 回调
    if (alertCb_) {
        alertCb_(event);
    }
}

bool AlertEngine::isInCooldown(const std::string& ruleName, int cooldownSec) const {
    std::lock_guard<std::mutex> lock(cooldownMutex_);
    auto it = cooldowns_.find(ruleName);
    if (it == cooldowns_.end()) return false;

    int64_t now = Timestamp::now().microSecondsSinceEpoch();
    double elapsed = (now - it->second) / 1e6;
    return elapsed < cooldownSec;
}

std::vector<AlertEvent> AlertEngine::getRecentAlerts(int limit) const {
    std::lock_guard<std::mutex> lock(alertsMutex_);
    std::vector<AlertEvent> result;
    int start = std::max(0, static_cast<int>(alertHistory_.size()) - limit);
    for (size_t i = start; i < alertHistory_.size(); ++i) {
        result.push_back(alertHistory_[i]);
    }
    return result;
}

size_t AlertEngine::activeAlertCount() const {
    std::lock_guard<std::mutex> lock(alertsMutex_);
    size_t count = 0;
    for (const auto& a : alertHistory_) {
        if (!a.resolved) count++;
    }
    return count;
}
