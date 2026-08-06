#include "TsdbClient.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <sstream>
#include <cstdio>

TsdbClient::TsdbClient(EventLoop* loop, const TsdbConfig& cfg)
    : loop_(loop)
    , config_(cfg)
    , connected_(false) {}

TsdbClient::~TsdbClient() {
    disconnect();
}

bool TsdbClient::connect() {
    // InfluxDB 不需要持久连接, 每次 HTTP POST 即可
    connected_ = true;
    LOG_INFO("TsdbClient: target=%s:%d db=%s",
             config_.host.c_str(), config_.port, config_.database.c_str());
    return true;
}

void TsdbClient::disconnect() {
    flush();  // 最后尝试发送剩余数据
    connected_ = false;
}

// ============================================================
// writeDeviceData — 异步写入设备数据
//
// 数据格式 (InfluxDB Line Protocol):
//   device_data,device_id=<id>,topic=<topic> value="<payload>" <timestamp_ns>
//
// 转义规则:
//   - measurement 中的逗号 → \,
//   - tag value 中的空格 → \
//   - field value 中的双引号 → \"
// ============================================================
void TsdbClient::writeDeviceData(const std::string& deviceId,
                                 const std::string& measurement,
                                 const std::string& value) {
    if (!connected_) return;

    // 转义: 将 value 中的双引号和反斜杠转义
    std::string escapedValue;
    escapedValue.reserve(value.size() + 8);
    for (char c : value) {
        if (c == '"' || c == '\\') escapedValue += '\\';
        escapedValue += c;
    }

    // 构建 Line Protocol 行
    char buf[4096];
    int64_t tsNs = Timestamp::now().microSecondsSinceEpoch() * 1000;

    int len = snprintf(buf, sizeof(buf),
        "device_data,device_id=%s,topic=%s value=\"%s\" %lld\n",
        deviceId.c_str(), measurement.c_str(),
        escapedValue.c_str(), (long long)tsNs);

    if (len < 0 || static_cast<size_t>(len) >= sizeof(buf)) {
        LOG_ERROR("TsdbClient: line too long for device=%s", deviceId.c_str());
        return;
    }

    batchBuffer_ += std::string(buf, len);
    batchCount_++;

    // 达到批量阈值 → flush
    if (batchCount_ >= kMaxBatchSize ||
        batchBuffer_.size() >= kMaxBatchBytes) {
        flush();
    }
}

// ============================================================
// flush — 批量发送到 InfluxDB
//
// HTTP POST 格式:
//   POST /write?db=<database>&precision=ns HTTP/1.1
//   Host: <host>:<port>
//   Content-Type: text/plain
//   Content-Length: <len>
//
//   <line1>
//   <line2>
//
// 当前为桩实现: 打印到日志, 不实际发送 HTTP
// 实际部署时可替换为 libcurl 异步 POST 或 TCP socket 发送
// ============================================================
void TsdbClient::flush() {
    if (batchBuffer_.empty()) return;

    LOG_DEBUG("TsdbClient: flushing %d lines (%zu bytes)",
              batchCount_, batchBuffer_.size());

    sendHttpPost(batchBuffer_);

    batchBuffer_.clear();
    batchCount_ = 0;
}

// ============================================================
// sendHttpPost — 简易 HTTP POST (实际部署时替换为 libcurl)
// ============================================================
void TsdbClient::sendHttpPost(const std::string& body) {
    // 桩实现: 实际部署时可替换为 libcurl HTTP POST 到 InfluxDB /write 端点
    LOG_TRACE("TsdbClient: flush %zu bytes to %s:%d/write?db=%s",
              body.size(), config_.host.c_str(), config_.port, config_.database.c_str());
}
