#pragma once
#include <string>
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <mutex>

// ============================================================
// 简易日志库 — 支持 TRACE/DEBUG/INFO/WARN/ERROR 五级
// 线程安全 · 可设置全局级别 · 输出到 stdout
// ============================================================
class Logger {
public:
    enum Level { TRACE = 0, DEBUG = 1, INFO = 2, WARN = 3, ERROR = 4 };

    static void setLevel(Level level) { s_level = level; }
    static Level level() { return s_level; }

    // 格式化日志输出
    static void log(Level lv, const char* file, int line,
                    const char* fmt, ...) {
        if (lv < s_level) return;

        std::lock_guard<std::mutex> guard(s_mutex);

        // 时间戳
        char timeBuf[32];
        time_t now = time(nullptr);
        struct tm tmNow;
#ifdef _WIN32
        localtime_s(&tmNow, &now);
#else
        localtime_r(&now, &tmNow);
#endif
        strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S", &tmNow);

        // 级别名称
        const char* levelNames[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
        fprintf(stdout, "[%s] %s %s:%d: ", timeBuf, levelNames[lv], file, line);

        // 用户消息
        va_list args;
        va_start(args, fmt);
        vfprintf(stdout, fmt, args);
        va_end(args);

        fprintf(stdout, "\n");
        fflush(stdout);
    }

private:
    static Level s_level;
    static std::mutex s_mutex;
};

// 在 Logger.cpp 中定义 (仅此一处, 使用 inline static 也可)
inline Logger::Level Logger::s_level = Logger::INFO;
inline std::mutex Logger::s_mutex;

// 便捷宏
#define LOG_TRACE(fmt, ...) Logger::log(Logger::TRACE, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) Logger::log(Logger::DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  Logger::log(Logger::INFO,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  Logger::log(Logger::WARN,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) Logger::log(Logger::ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
