#include "MetricsCollector.h"
#include "net/EventLoop.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <sstream>
#include <algorithm>
#include <cmath>

MetricsCollector::MetricsCollector(EventLoop* loop) : loop_(loop) {}
MetricsCollector::~MetricsCollector() { stop(); }

void MetricsCollector::record(const std::string& name, double value,
                             const std::unordered_map<std::string, std::string>& tags) {
    std::lock_guard<std::mutex> lock(mutex_);

    MetricSample sample;
    sample.name = name;
    sample.value = value;
    sample.timestamp = Timestamp::now().microSecondsSinceEpoch();
    sample.tags = tags;

    auto& data = data_[name];
    data.samples.push_back(sample);
    data.latestValue = value;

    // 限制样本数量
    if (data.samples.size() > kMaxSamplesPerMetric) {
        data.samples.erase(data.samples.begin());
    }
}

void MetricsCollector::increment(const std::string& name, double delta) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& data = data_[name];
    data.counterValue += delta;
    data.latestValue = data.counterValue;

    MetricSample sample;
    sample.name = name;
    sample.value = data.counterValue;
    sample.timestamp = Timestamp::now().microSecondsSinceEpoch();

    data.samples.push_back(sample);
    if (data.samples.size() > kMaxSamplesPerMetric) {
        data.samples.erase(data.samples.begin());
    }
}

void MetricsCollector::setGauge(const std::string& name, double value) {
    record(name, value);
}

double MetricsCollector::getLatest(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(name);
    return it != data_.end() ? it->second.latestValue : 0.0;
}

AggregatedMetric MetricsCollector::getAggregated(const std::string& name,
                                                 int windowSec) const {
    AggregatedMetric agg;
    agg.name = name;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(name);
    if (it == data_.end() || it->second.samples.empty()) return agg;

    int64_t now = Timestamp::now().microSecondsSinceEpoch();
    int64_t cutoff = now - windowSec * 1000000LL;
    agg.windowStart = cutoff;
    agg.windowEnd = now;

    // 收集窗口内的样本
    std::vector<double> values;
    for (const auto& s : it->second.samples) {
        if (s.timestamp >= cutoff) {
            values.push_back(s.value);
        }
    }

    if (values.empty()) return agg;

    agg.sampleCount = values.size();

    // 排序以计算分位数
    std::sort(values.begin(), values.end());

    agg.min = values.front();
    agg.max = values.back();

    double sum = 0;
    for (double v : values) sum += v;
    agg.avg = sum / values.size();

    auto percentile = [&values](double p) -> double {
        size_t idx = static_cast<size_t>(std::ceil(p * values.size())) - 1;
        if (idx >= values.size()) idx = values.size() - 1;
        return values[idx];
    };

    agg.p50 = percentile(0.50);
    agg.p95 = percentile(0.95);
    agg.p99 = percentile(0.99);

    return agg;
}

std::vector<std::string> MetricsCollector::getMetricNames() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    for (const auto& [name, _] : data_) {
        names.push_back(name);
    }
    return names;
}

std::string MetricsCollector::toJson() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ostringstream oss;
    oss << "{";
    bool first = true;
    for (const auto& [name, d] : data_) {
        if (!first) oss << ",";
        oss << "\"" << name << "\":" << d.latestValue;
        first = false;
    }
    oss << "}";
    return oss.str();
}

void MetricsCollector::start(int intervalSec) {
    running_ = true;
    // 定期清理过期样本 (每小时)
    collectTimerId_ = loop_->runEvery(
        static_cast<double>(intervalSec > 0 ? intervalSec : 300),
        [this]() { pruneOldSamples(); });
    LOG_INFO("MetricsCollector: started (prune interval=%ds)", intervalSec);
}

void MetricsCollector::stop() {
    running_ = false;
    if (collectTimerId_ > 0) {
        loop_->cancelTimer(collectTimerId_);
        collectTimerId_ = 0;
    }
}

void MetricsCollector::pruneOldSamples() {
    // 由定时器调用, 清理过期样本
    int64_t cutoff = Timestamp::now().microSecondsSinceEpoch() - 3600 * 1000000LL;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [name, d] : data_) {
        d.samples.erase(
            std::remove_if(d.samples.begin(), d.samples.end(),
                [cutoff](const MetricSample& s) { return s.timestamp < cutoff; }),
            d.samples.end());
    }
}
