// ============================================================
// simulator_main.cpp — 物联网设备模拟器
//
// 功能:
//   - 模拟 N 个 MQTT 设备并发连接网关
//   - 每个设备定时上报遥测数据 (PUBLISH)
//   - 支持参数化并发数和上报频率
//   - 用于压测网关性能
//
// 压测建议:
//   1. 先用小规模 (100 设备) 验证链路正确性
//   2. 逐步增大并发量到 1万/5万/10万
//   3. 观察网关内存、CPU、连接数
//   4. 记录 QPS 和指令下发延迟
//
// 使用方法:
//   ./device_simulator --host 127.0.0.1 --port 1883 \
//       --devices 1000 --interval 5 --qps 100
// ============================================================

#include "net/EventLoop.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"
#include "net/InetAddress.h"
#include "protocol/MqttCodec.h"
#include "common/Logger.h"
#include "common/Timestamp.h"
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <thread>

// ============================================================
// 模拟设备
// ============================================================
struct SimDevice {
    std::string clientId;
    std::string username;
    bool        connected;
    int64_t     lastPublishTime;
    int         publishInterval;  // 秒
    int         msgCount;

    // 模拟的传感器数据
    double temperature;
    double humidity;
};

// ============================================================
// 模拟器配置
// ============================================================
struct SimConfig {
    std::string host = "127.0.0.1";
    uint16_t    port = 1883;
    int         deviceCount = 100;      // 设备数量
    int         publishInterval = 5;    // 上报间隔 (秒)
    int         connectRate = 50;       // 每秒建立连接数 (限速)
    bool        keepRunning = true;     // 持续运行
};

// ============================================================
// MQTT 客户端 (基于我们自己的网络库)
// ============================================================
class MqttClient : public std::enable_shared_from_this<MqttClient> {
public:
    MqttClient(EventLoop* loop, const std::string& clientId)
        : loop_(loop), clientId_(clientId), connected_(false) {}

    void connect(const InetAddress& serverAddr) {
        int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        int ret = ::connect(sockfd, reinterpret_cast<const sockaddr*>(
            &serverAddr.getSockAddr()), sizeof(sockaddr_in));
        if (ret < 0 && errno != EINPROGRESS) {
            LOG_ERROR("[%s] connect failed: %s", clientId_.c_str(), strerror(errno));
            ::close(sockfd);
            return;
        }

        conn_ = std::make_shared<TcpConnection>(loop_, clientId_, sockfd,
            InetAddress(0), serverAddr);
        conn_->setConnectionCallback(
            [this](const TcpConnectionPtr& conn) {
                if (conn->connected()) {
                    connected_ = true;
                    sendConnect();
                    LOG_INFO("[%s] connected to gateway", clientId_.c_str());
                } else {
                    connected_ = false;
                    LOG_WARN("[%s] disconnected", clientId_.c_str());
                }
            });
        conn_->setMessageCallback(
            [this](const TcpConnectionPtr&, Buffer* buf, Timestamp) {
                handleMessage(buf);
            });
        conn_->connectEstablished();
    }

    void publish(const std::string& topic, const std::string& payload) {
        if (!connected_) return;
        Buffer buf;
        MqttCodec::encodePublish(&buf, topic, payload, MqttQoS::AT_LEAST_ONCE, 1);
        conn_->send(&buf);
        msgCount_++;
    }

    void disconnect() {
        if (conn_) {
            conn_->shutdown();
        }
    }

    bool isConnected() const { return connected_; }
    int messageCount() const { return msgCount_; }

private:
    void sendConnect() {
        // 构建 CONNECT 报文
        Buffer buf;
        uint8_t connectByte = mqttPacketTypeByte(MqttPacketType::CONNECT);
        uint8_t flags = MQTT_CONNECT_FLAG_CLEAN_SESSION;
        uint16_t keepAlive = htons(60);

        // 手动构建 CONNECT 报文
        // Remaining Length = 10 (可变头) + 2 + clientId.length()
        uint32_t remaining = 10 + 2 + clientId_.size();

        buf.append(&connectByte, 1);

        // Remaining Length 编码
        uint8_t rlBytes[4];
        int rlLen = 0;
        uint32_t tmp = remaining;
        do {
            rlBytes[rlLen] = tmp % 128;
            tmp /= 128;
            if (tmp > 0) rlBytes[rlLen] |= 0x80;
            rlLen++;
        } while (tmp > 0);
        buf.append(rlBytes, rlLen);

        // Variable Header
        const char protocolName[] = {0x00, 0x04, 'M', 'Q', 'T', 'T'};
        buf.append(protocolName, 6);  // "MQTT"
        uint8_t protoLevel = 4;
        buf.append(&protoLevel, 1);
        buf.append(&flags, 1);
        buf.append(&keepAlive, 2);

        // Client ID
        uint16_t idLen = htons(static_cast<uint16_t>(clientId_.size()));
        buf.append(&idLen, 2);
        buf.append(clientId_);

        conn_->send(&buf);
    }

    void handleMessage(Buffer* buf) {
        // 简单处理: 消费 CONNACK
        // 完整实现应使用 MqttCodec
        if (buf->readableBytes() >= 4) {
            buf->retrieve(buf->readableBytes());
        }
    }

    EventLoop* loop_;
    std::string clientId_;
    TcpConnectionPtr conn_;
    bool connected_;
    int msgCount_ = 0;
};

// ============================================================
// 主函数
// ============================================================
int main(int argc, char* argv[]) {
    SimConfig cfg;

    // ---- 解析参数 ----
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            cfg.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            cfg.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--devices" && i + 1 < argc) {
            cfg.deviceCount = std::atoi(argv[++i]);
        } else if (arg == "--interval" && i + 1 < argc) {
            cfg.publishInterval = std::atoi(argv[++i]);
        } else if (arg == "--rate" && i + 1 < argc) {
            cfg.connectRate = std::atoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            printf("IoT Device Simulator\n");
            printf("Usage: %s [options]\n", argv[0]);
            printf("  --host <host>         Gateway host (default: 127.0.0.1)\n");
            printf("  --port <port>         Gateway MQTT port (default: 1883)\n");
            printf("  --devices <num>       Number of devices (default: 100)\n");
            printf("  --interval <sec>      Publish interval (default: 5)\n");
            printf("  --rate <num/sec>      Connect rate limit (default: 50)\n");
            return 0;
        }
    }

    Logger::setLevel(Logger::INFO);
    LOG_INFO("========================================");
    LOG_INFO("IoT Device Simulator");
    LOG_INFO("========================================");
    LOG_INFO("Target:    %s:%u", cfg.host.c_str(), cfg.port);
    LOG_INFO("Devices:   %d", cfg.deviceCount);
    LOG_INFO("Interval:  %ds", cfg.publishInterval);
    LOG_INFO("Rate:      %d conn/s", cfg.connectRate);
    LOG_INFO("========================================");

    // ---- 准备 ----
    EventLoop loop;
    InetAddress serverAddr(cfg.host, cfg.port);
    std::vector<std::shared_ptr<MqttClient>> clients;
    clients.reserve(cfg.deviceCount);

    // ---- 模拟传感器数据模板 ----
    std::vector<std::string> topics = {
        "/sensor/temperature",
        "/sensor/humidity",
        "/sensor/pressure",
        "/device/status",
    };

    std::mt19937 rng(static_cast<unsigned>(std::chrono::steady_clock::now()
                                              .time_since_epoch().count()));
    std::uniform_real_distribution<double> tempDist(15.0, 35.0);
    std::uniform_real_distribution<double> humDist(30.0, 90.0);

    // ---- 分批连接设备 ----
    LOG_INFO("Connecting %d devices at %d conn/s...", cfg.deviceCount, cfg.connectRate);

    int64_t startTime = Timestamp::now().microSecondsSinceEpoch();

    for (int i = 0; i < cfg.deviceCount; ++i) {
        char idBuf[32];
        snprintf(idBuf, sizeof(idBuf), "sim-device-%05d", i);
        auto client = std::make_shared<MqttClient>(&loop, idBuf);
        client->connect(serverAddr);
        clients.push_back(client);

        // 限速: 每秒最多 cfg.connectRate 个连接
        if ((i + 1) % cfg.connectRate == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if ((i + 1) % 100 == 0) {
            LOG_INFO("  Connected %d/%d devices...", i + 1, cfg.deviceCount);
        }
    }

    int64_t endTime = Timestamp::now().microSecondsSinceEpoch();
    double elapsed = (endTime - startTime) / 1e6;
    LOG_INFO("All %d devices connected in %.1fs", cfg.deviceCount, elapsed);

    // ---- 定时发布数据 ----
    LOG_INFO("Starting data publishing (interval=%ds)...", cfg.publishInterval);

    int publishRounds = 0;
    int64_t lastPublishTime = Timestamp::now().microSecondsSinceEpoch();

    loop.runEvery(cfg.publishInterval, [&]() {
        publishRounds++;
        int totalMsgs = 0;

        for (auto& client : clients) {
            if (!client->isConnected()) continue;

            // 随机选一个 topic
            std::string& topic = topics[publishRounds % topics.size()];

            // 生成模拟数据
            char payload[128];
            double temp = tempDist(rng);
            double hum = humDist(rng);
            snprintf(payload, sizeof(payload),
                     R"({"temperature":%.1f,"humidity":%.1f,"ts":%lld})",
                     temp, hum,
                     (long long)Timestamp::now().microSecondsSinceEpoch());

            client->publish(topic, payload);
            totalMsgs++;
        }

        // 统计
        int connected = 0;
        for (auto& c : clients) { if (c->isConnected()) connected++; }

        LOG_INFO("Round #%d: %d msgs published, %d/%zu devices online",
                 publishRounds, totalMsgs, connected, clients.size());
    });

    // ---- 定期统计 ----
    loop.runEvery(10.0, [&]() {
        int connected = 0, totalMsgs = 0;
        for (auto& c : clients) {
            if (c->isConnected()) connected++;
            totalMsgs += c->messageCount();
        }
        LOG_INFO("=== Stats: %d/%zu devices online, %d total msgs sent ===",
                 connected, clients.size(), totalMsgs);
    });

    // ---- 进入事件循环 ----
    LOG_INFO("Simulator running. Press Ctrl+C to stop.");
    loop.loop();

    // ---- 清理 ----
    LOG_INFO("Disconnecting all devices...");
    for (auto& client : clients) {
        client->disconnect();
    }
    LOG_INFO("Simulator exited.");

    return 0;
}
