#pragma once
#include <cstdint>
#include <string>
#include <chrono>
#include <ctime>

// ============================================================
// 微秒精度时间戳工具类
// ============================================================
class Timestamp {
public:
    Timestamp() : microSeconds_(0) {}
    explicit Timestamp(int64_t microSec) : microSeconds_(microSec) {}

    static Timestamp now() {
        auto now = std::chrono::system_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch());
        return Timestamp(dur.count());
    }

    static Timestamp invalid() { return Timestamp(); }

    // ---- 只读 ----
    int64_t microSecondsSinceEpoch() const { return microSeconds_; }
    bool valid() const { return microSeconds_ > 0; }

    // ---- 比较 ----
    bool operator<(Timestamp rhs)  const { return microSeconds_ <  rhs.microSeconds_; }
    bool operator>(Timestamp rhs)  const { return microSeconds_ >  rhs.microSeconds_; }
    bool operator==(Timestamp rhs) const { return microSeconds_ == rhs.microSeconds_; }

    // ---- 算术 ----
    Timestamp operator+(double seconds) const {
        return Timestamp(microSeconds_ + static_cast<int64_t>(seconds * 1e6));
    }

    Timestamp operator-(double seconds) const {
        return Timestamp(microSeconds_ - static_cast<int64_t>(seconds * 1e6));
    }

    double operator-(Timestamp rhs) const {
        return static_cast<double>(microSeconds_ - rhs.microSeconds_) / 1e6;
    }

    // ---- 格式化 ----
    std::string toString() const {
        time_t sec = static_cast<time_t>(microSeconds_ / 1000000);
        int us = static_cast<int>(microSeconds_ % 1000000);
        struct tm tmResult;
#ifdef _WIN32
        localtime_s(&tmResult, &sec);
#else
        localtime_r(&sec, &tmResult);
#endif
        char buf[64];
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                 tmResult.tm_year + 1900, tmResult.tm_mon + 1,
                 tmResult.tm_mday,
                 tmResult.tm_hour, tmResult.tm_min, tmResult.tm_sec, us);
        return buf;
    }

private:
    int64_t microSeconds_;  // 自 epoch 的微秒数
};
