#pragma once

// ============================================================
// NonCopyable — 禁用拷贝构造和拷贝赋值的基类
// 用法: class MyClass : private NonCopyable { ... };
// ============================================================
class NonCopyable {
public:
    NonCopyable() = default;
    ~NonCopyable() = default;

    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;

    // 移动构造/赋值允许 (需要的话子类自己决定)
    NonCopyable(NonCopyable&&) = default;
    NonCopyable& operator=(NonCopyable&&) = default;
};
