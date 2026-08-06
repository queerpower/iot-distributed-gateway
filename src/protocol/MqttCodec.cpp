#include "MqttCodec.h"
#include "common/Logger.h"
#include <arpa/inet.h>

// ============================================================
// 解码: Remaining Length (变长整数)
//
// 编码规则:
//   - 每字节的低 7 位是数值, 最高位 (bit7) = 1 表示后面还有字节
//   - 最多 4 字节, 最大 0xFF,0xFF,0xFF,0x7F = 268,435,455
// ============================================================
uint32_t MqttCodec::decodeRemainingLength(const char* data, size_t len,
                                          size_t* consumed) {
    uint32_t value = 0;
    int multiplier = 1;
    size_t i = 0;
    for (; i < len && i < 4; ++i) {
        uint8_t byte = static_cast<uint8_t>(data[i]);
        value += (byte & 0x7F) * multiplier;
        multiplier *= 128;
        if (!(byte & 0x80)) break;  // 最高位为 0 → 结束
    }
    *consumed = i + 1;
    return value;
}

// ============================================================
// 编码: Remaining Length
// ============================================================
void MqttCodec::encodeRemainingLength(Buffer* out, uint32_t len) {
    do {
        uint8_t byte = len % 128;
        len /= 128;
        if (len > 0) byte |= 0x80;
        out->append(&byte, 1);
    } while (len > 0);
}

// ============================================================
// MQTT 字符串 — 2 字节大端长度 + UTF-8 内容
// ============================================================
std::string MqttCodec::readMqttString(const char*& p, const char* end) {
    if (p + 2 > end) return "";
    uint16_t len = ntohs(*reinterpret_cast<const uint16_t*>(p));
    p += 2;
    if (p + len > end) return "";
    std::string result(p, len);
    p += len;
    return result;
}

void MqttCodec::writeMqttString(Buffer* out, const std::string& str) {
    uint16_t len = htons(static_cast<uint16_t>(str.size()));
    out->append(&len, 2);
    out->append(str);
}

// ============================================================
// parse — 入口: 尝试从 Buffer 解析一个完整报文
// ============================================================
MqttCodec::ParseResult MqttCodec::parse(Buffer* buf) {
    // 至少需要 2 字节 (1 字节类型 + 至少 1 字节 Remaining Length)
    if (buf->readableBytes() < 2) return ParseResult::INCOMPLETE;

    const char* p = buf->peek();
    const char* end = p + buf->readableBytes();

    uint8_t firstByte = static_cast<uint8_t>(p[0]);
    MqttPacketType type = mqttPacketTypeFromByte(firstByte);
    uint8_t flags = mqttPacketFlags(firstByte);

    // 解码 Remaining Length
    size_t rlBytes = 0;
    uint32_t remainingLen = decodeRemainingLength(p + 1, end - p - 1, &rlBytes);

    size_t totalLen = 1 + rlBytes + remainingLen;
    if (buf->readableBytes() < totalLen) return ParseResult::INCOMPLETE;

    // 凑齐报文了, 指针移到 Variable Header 开始处
    const char* data = p + 1 + rlBytes;
    const char* payloadEnd = data + remainingLen;

    // ---- 根据类型分发 ----
    ParseResult result = ParseResult::OK;
    switch (type) {
    case MqttPacketType::CONNECT:
        result = parseConnect(data, remainingLen);
        break;
    case MqttPacketType::PUBLISH:
        result = parsePublish(flags, data, remainingLen);
        break;
    case MqttPacketType::PUBACK:
        result = parsePubAck(data, remainingLen);
        break;
    case MqttPacketType::SUBSCRIBE:
        result = parseSubscribe(data, remainingLen);
        break;
    case MqttPacketType::PINGREQ:
        if (pingreqCb_) pingreqCb_();
        break;
    case MqttPacketType::DISCONNECT:
        if (disconnectCb_) disconnectCb_();
        break;
    default:
        LOG_WARN("MqttCodec: unhandled packet type %d", static_cast<int>(type));
        break;
    }

    // 消费已解析的数据
    buf->retrieve(totalLen);
    return result;
}

// ============================================================
// parseConnect — 解析 CONNECT 报文
// ============================================================
MqttCodec::ParseResult MqttCodec::parseConnect(const char* data,
                                               size_t remaining) {
    const char* p = data;
    const char* end = data + remaining;

    MqttConnectPayload payload;

    // 协议名长度 + 协议名
    payload.protocolName = readMqttString(p, end);
    if (p >= end) return ParseResult::ERROR;

    // 协议级别
    payload.protocolLevel = static_cast<uint8_t>(*p++);
    if (p >= end) return ParseResult::ERROR;

    // Connect Flags
    payload.flags = static_cast<uint8_t>(*p++);
    if (p >= end) return ParseResult::ERROR;

    // Keep Alive
    payload.keepAlive = ntohs(*reinterpret_cast<const uint16_t*>(p));
    p += 2;
    if (p > end) return ParseResult::ERROR;

    // Client ID
    payload.clientId = readMqttString(p, end);

    // Will Topic & Message (如果有遗嘱标志)
    if (payload.flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        payload.willTopic = readMqttString(p, end);
        payload.willMessage = readMqttString(p, end);
    }

    // Username
    if (payload.flags & MQTT_CONNECT_FLAG_USERNAME) {
        payload.username = readMqttString(p, end);
    }

    // Password
    if (payload.flags & MQTT_CONNECT_FLAG_PASSWORD) {
        payload.password = readMqttString(p, end);
    }

    LOG_INFO("MqttCodec: CONNECT clientId=%s keepAlive=%d",
             payload.clientId.c_str(), payload.keepAlive);

    if (connectCb_) connectCb_(payload);
    return ParseResult::OK;
}

// ============================================================
// parsePublish — 解析 PUBLISH 报文
//
// Flags (低 4 bit, MQTT 3.1.1):
//   Bit 3: DUP
//   Bit 2-1: QoS
//   Bit 0: RETAIN
// ============================================================
MqttCodec::ParseResult MqttCodec::parsePublish(uint8_t flags,
                                               const char* data,
                                               size_t remaining) {
    const char* p = data;
    const char* end = data + remaining;

    bool retain = flags & 0x01;
    uint8_t qosVal = (flags >> 1) & 0x03;
    bool dup = flags & 0x08;
    MqttQoS qos = static_cast<MqttQoS>(qosVal);

    // Topic
    std::string topic = readMqttString(p, end);

    // Packet Identifier (仅 QoS > 0)
    uint16_t packetId = 0;
    if (qos > MqttQoS::AT_MOST_ONCE) {
        if (p + 2 > end) return ParseResult::ERROR;
        packetId = ntohs(*reinterpret_cast<const uint16_t*>(p));
        p += 2;
    }

    // Payload (剩余全部)
    std::string payload(p, end - p);

    if (publishCb_) publishCb_(topic, payload, qos, retain, packetId);
    return ParseResult::OK;
}

// ============================================================
// parseSubscribe — 解析 SUBSCRIBE 报文
// ============================================================
MqttCodec::ParseResult MqttCodec::parseSubscribe(const char* data,
                                                 size_t remaining) {
    const char* p = data;
    const char* end = data + remaining;

    // Packet Identifier (2 字节)
    if (p + 2 > end) return ParseResult::ERROR;
    uint16_t packetId = ntohs(*reinterpret_cast<const uint16_t*>(p));
    p += 2;

    // Topic Filters
    std::vector<MqttTopicFilter> filters;
    while (p < end) {
        std::string topic = readMqttString(p, end);
        if (p >= end) return ParseResult::ERROR;
        uint8_t qos = static_cast<uint8_t>(*p++);
        filters.push_back({topic, qos});
    }

    LOG_INFO("MqttCodec: SUBSCRIBE packetId=%u topics=%zu",
             packetId, filters.size());

    if (subscribeCb_) subscribeCb_(packetId, filters);
    return ParseResult::OK;
}

// ============================================================
// parsePubAck — 解析 PUBACK
// ============================================================
MqttCodec::ParseResult MqttCodec::parsePubAck(const char* data,
                                              size_t remaining) {
    if (remaining < 2) return ParseResult::ERROR;
    uint16_t packetId = ntohs(*reinterpret_cast<const uint16_t*>(data));
    if (pubackCb_) pubackCb_(packetId);
    return ParseResult::OK;
}

// ============================================================
// 编码: 各种应答报文
// ============================================================

void MqttCodec::encodeConnAck(Buffer* out, MqttConnAckCode code) {
    uint8_t type = mqttPacketTypeByte(MqttPacketType::CONNACK);
    uint8_t remainingLen = 2;  // session present + return code

    out->append(&type, 1);
    encodeRemainingLength(out, remainingLen);

    // Variable Header
    uint8_t sessionPresent = 0;
    out->append(&sessionPresent, 1);
    uint8_t rc = static_cast<uint8_t>(code);
    out->append(&rc, 1);
}

void MqttCodec::encodeSubAck(Buffer* out, uint16_t packetId,
                             const std::vector<uint8_t>& returnCodes) {
    uint8_t type = mqttPacketTypeByte(MqttPacketType::SUBACK);
    uint32_t remainingLen = 2 + returnCodes.size();

    out->append(&type, 1);
    encodeRemainingLength(out, remainingLen);

    // Packet ID
    uint16_t nid = htons(packetId);
    out->append(&nid, 2);

    // Return codes
    for (uint8_t rc : returnCodes) {
        out->append(&rc, 1);
    }
}

void MqttCodec::encodePubAck(Buffer* out, uint16_t packetId) {
    uint8_t type = mqttPacketTypeByte(MqttPacketType::PUBACK);
    uint8_t remainingLen = 2;  // packet id

    out->append(&type, 1);
    encodeRemainingLength(out, remainingLen);

    uint16_t nid = htons(packetId);
    out->append(&nid, 2);
}

void MqttCodec::encodePublish(Buffer* out, const std::string& topic,
                              const std::string& payload, MqttQoS qos,
                              uint16_t packetId, bool retain) {
    uint8_t flags = 0;
    if (retain) flags |= 0x01;
    flags |= (static_cast<uint8_t>(qos) << 1);

    uint8_t type = mqttPacketTypeByte(MqttPacketType::PUBLISH, flags);

    // 计算 remaining length
    uint32_t remainingLen = 2 + topic.size();  // topic length + topic
    if (qos > MqttQoS::AT_MOST_ONCE) remainingLen += 2;  // packetId
    remainingLen += payload.size();

    out->append(&type, 1);
    encodeRemainingLength(out, remainingLen);

    writeMqttString(out, topic);

    if (qos > MqttQoS::AT_MOST_ONCE) {
        uint16_t nid = htons(packetId);
        out->append(&nid, 2);
    }

    out->append(payload);
}

void MqttCodec::encodePingResp(Buffer* out) {
    uint8_t type = mqttPacketTypeByte(MqttPacketType::PINGRESP);
    uint8_t remainingLen = 0;
    out->append(&type, 1);
    out->append(&remainingLen, 1);
}

void MqttCodec::encodeUnsubAck(Buffer* out, uint16_t packetId) {
    uint8_t type = mqttPacketTypeByte(MqttPacketType::UNSUBACK);
    uint8_t remainingLen = 2;

    out->append(&type, 1);
    encodeRemainingLength(out, remainingLen);

    uint16_t nid = htons(packetId);
    out->append(&nid, 2);
}
