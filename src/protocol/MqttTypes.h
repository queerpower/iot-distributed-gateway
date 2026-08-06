#pragma once
#include <cstdint>
#include <string>

// ============================================================
// MQTT 3.1.1 协议 — 类型定义
//
// 报文结构:
//   Fixed Header (2-5 bytes) + Variable Header + Payload
//
// Fixed Header:
//   Byte 1:  Message Type (4 bit) + Flags (4 bit)
//   Byte 2+: Remaining Length (1-4 bytes, 变长编码)
// ============================================================

// ---- 报文类型 (4 bit) ----
enum class MqttPacketType : uint8_t {
    CONNECT     = 1,
    CONNACK     = 2,
    PUBLISH     = 3,
    PUBACK      = 4,
    PUBREC      = 5,
    PUBREL      = 6,
    PUBCOMP     = 7,
    SUBSCRIBE   = 8,
    SUBACK      = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK    = 11,
    PINGREQ     = 12,
    PINGRESP    = 13,
    DISCONNECT  = 14,
};

// ---- CONNECT 报文 Flags ----
constexpr uint8_t MQTT_CONNECT_FLAG_USERNAME  = 0x80;
constexpr uint8_t MQTT_CONNECT_FLAG_PASSWORD  = 0x40;
constexpr uint8_t MQTT_CONNECT_FLAG_WILL_RETAIN = 0x20;
constexpr uint8_t MQTT_CONNECT_FLAG_WILL_FLAG  = 0x04;
constexpr uint8_t MQTT_CONNECT_FLAG_CLEAN_SESSION = 0x02;

// ---- CONNACK 返回码 ----
enum class MqttConnAckCode : uint8_t {
    ACCEPTED            = 0,
    UNACCEPTABLE_PROTOCOL = 1,
    IDENTIFIER_REJECTED = 2,
    SERVER_UNAVAILABLE  = 3,
    BAD_USERNAME_PASSWORD = 4,
    NOT_AUTHORIZED      = 5,
};

// ---- QoS 等级 ----
enum class MqttQoS : uint8_t {
    AT_MOST_ONCE  = 0,
    AT_LEAST_ONCE = 1,
    EXACTLY_ONCE  = 2,
};

// ---- SUBACK 返回码 ----
constexpr uint8_t MQTT_SUBACK_SUCCESS_QOS0 = 0x00;
constexpr uint8_t MQTT_SUBACK_SUCCESS_QOS1 = 0x01;
constexpr uint8_t MQTT_SUBACK_SUCCESS_QOS2 = 0x02;
constexpr uint8_t MQTT_SUBACK_FAILURE      = 0x80;

// ============================================================
// MqttFixedHeader — 固定报头
// ============================================================
struct MqttFixedHeader {
    MqttPacketType type;
    uint8_t        flags;          // 低 4 bit, 不同类型的含义不同
    uint32_t       remainingLen;   // 解码后的实际长度

    MqttFixedHeader() : type(MqttPacketType::PINGREQ), flags(0), remainingLen(0) {}
};

// ============================================================
// MqttConnectPayload — CONNECT 报文内容
// ============================================================
struct MqttConnectPayload {
    std::string protocolName;   // 固定 "MQTT"
    uint8_t     protocolLevel;  // 4 表示 MQTT 3.1.1
    uint8_t     flags;          // connect flags
    uint16_t    keepAlive;      // 保活时间 (秒)
    std::string clientId;
    std::string willTopic;
    std::string willMessage;
    std::string username;
    std::string password;

    MqttConnectPayload() : protocolLevel(4), flags(0), keepAlive(60) {}
};

// ============================================================
// MqttSubscribePacket — SUBSCRIBE 订阅主题
// ============================================================
struct MqttTopicFilter {
    std::string topic;
    uint8_t     qos;  // 期望的 QoS
};

// ============================================================
// MqttPublishPacket — PUBLISH 发布消息
// ============================================================
struct MqttPublishPacket {
    std::string topic;
    std::string payload;
    MqttQoS     qos;
    bool        retain;
    bool        dup;
    uint16_t    packetId;  // QoS > 0 时有意义
};

// ============================================================
// 消息类型标识 (带载 4bit) 组合值
// ============================================================
inline uint8_t mqttPacketTypeByte(MqttPacketType type, uint8_t flags = 0) {
    return (static_cast<uint8_t>(type) << 4) | (flags & 0x0F);
}

inline MqttPacketType mqttPacketTypeFromByte(uint8_t byte) {
    return static_cast<MqttPacketType>(byte >> 4);
}

inline uint8_t mqttPacketFlags(uint8_t byte) {
    return byte & 0x0F;
}
