#pragma once
#include "MqttTypes.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include <functional>
#include <memory>
#include <cstring>

// ============================================================
// MqttCodec — MQTT 3.1.1 报文编解码器
//
// 职责:
//  1. 从 Buffer 中解析完整的 MQTT 报文 (处理 TCP 粘包/半包)
//  2. 构造各类 MQTT 应答报文
//  3. 为 MQTT 报文提供回调分发
//
// 粘包处理:
//  - 解析 Fixed Header 的 Remaining Length 得到报文全长
//  - 检查 Buffer 中是否有足够字节, 不够则返回等待更多数据
//  - 凑齐后再解析 Variable Header + Payload
//
// Remaining Length 变长编码:
//  - 每个字节的低 7 位是数据, 最高位是延续标志
//  - 最多 4 个字节 → 最大 268,435,455 字节 (256MB)
// ============================================================
class MqttCodec {
public:
    // 解析结果
    enum class ParseResult {
        OK,           // 解析成功
        INCOMPLETE,   // 数据不够, 等更多数据
        ERROR,        // 协议错误
    };

    // 回调: 收到各种 MQTT 报文时触发
    using ConnectCallback    = std::function<void(const MqttConnectPayload&)>;
    using PublishCallback    = std::function<void(const std::string& topic,
                                                  const std::string& payload,
                                                  MqttQoS qos, bool retain,
                                                  uint16_t packetId)>;
    using SubscribeCallback  = std::function<void(uint16_t packetId,
                                                  const std::vector<MqttTopicFilter>&)>;
    using PingReqCallback    = std::function<void()>;
    using DisconnectCallback = std::function<void()>;
    using PubAckCallback     = std::function<void(uint16_t packetId)>;

    MqttCodec() = default;

    // ---- 设置回调 ----
    void setConnectCallback(ConnectCallback cb)     { connectCb_ = std::move(cb); }
    void setPublishCallback(PublishCallback cb)     { publishCb_ = std::move(cb); }
    void setSubscribeCallback(SubscribeCallback cb) { subscribeCb_ = std::move(cb); }
    void setPingReqCallback(PingReqCallback cb)     { pingreqCb_ = std::move(cb); }
    void setDisconnectCallback(DisconnectCallback cb) { disconnectCb_ = std::move(cb); }
    void setPubAckCallback(PubAckCallback cb)       { pubackCb_ = std::move(cb); }

    // ---- 解码: 从 Buffer 中尝试解析一个完整报文 ----
    ParseResult parse(Buffer* buf);

    // ---- 编码: 构造应答报文 ----
    // CONNACK
    static void encodeConnAck(Buffer* out, MqttConnAckCode code);
    // SUBACK
    static void encodeSubAck(Buffer* out, uint16_t packetId,
                             const std::vector<uint8_t>& returnCodes);
    // PUBACK (QoS 1 确认)
    static void encodePubAck(Buffer* out, uint16_t packetId);
    // PUBLISH (网关下发/转发)
    static void encodePublish(Buffer* out, const std::string& topic,
                              const std::string& payload, MqttQoS qos,
                              uint16_t packetId, bool retain = false);
    // PINGRESP
    static void encodePingResp(Buffer* out);
    // UNSUBACK
    static void encodeUnsubAck(Buffer* out, uint16_t packetId);

private:
    // ---- Remaining Length 编解码 ----
    static uint32_t decodeRemainingLength(const char* data, size_t len,
                                          size_t* consumed);
    static void     encodeRemainingLength(Buffer* out, uint32_t len);

    // ---- 读取 MQTT 字符串 (2 字节长度前缀 + UTF-8) ----
    static std::string readMqttString(const char*& p, const char* end);

    // ---- 写入 MQTT 字符串 ----
    static void writeMqttString(Buffer* out, const std::string& str);

    // ---- 具体报文解析 ----
    ParseResult parseConnect(const char* data, size_t remaining);
    ParseResult parsePublish(uint8_t flags, const char* data, size_t remaining);
    ParseResult parseSubscribe(const char* data, size_t remaining);
    ParseResult parsePubAck(const char* data, size_t remaining);

    // ---- 回调 ----
    ConnectCallback    connectCb_;
    PublishCallback    publishCb_;
    SubscribeCallback  subscribeCb_;
    PingReqCallback    pingreqCb_;
    DisconnectCallback disconnectCb_;
    PubAckCallback     pubackCb_;
};
