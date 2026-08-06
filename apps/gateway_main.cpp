// ============================================================
// gateway_main.cpp — 分布式IoT设备监控与指令调度平台
//                     网关节点主程序入口
//
// 启动流程:
//   1. 解析命令行参数 (--node-id, --port, --api-port, --redis, etc.)
//   2. 初始化 EventLoop (主 Reactor)
//   3. 初始化 GatewayServer (MQTT 设备接入)
//   4. 初始化 ApiServer (HTTP REST API)
//   5. 加入 Redis 集群 (分布式注册表 + 消费者组)
//   6. 进入事件循环
//
// 多节点部署示例:
//   # 节点1 (入口通过 Nginx L4 LB)
//   ./gateway_server --node-id gateway-1 --port 1883 --api-port 8081 \
//       --redis 192.168.1.10:6379
//
//   # 节点2
//   ./gateway_server --node-id gateway-2 --port 1884 --api-port 8082 \
//       --redis 192.168.1.10:6379
//
//   # 节点3
//   ./gateway_server --node-id gateway-3 --port 1885 --api-port 8083 \
//       --redis 192.168.1.10:6379
//
// Nginx L4 负载均衡配置 (nginx.conf):
//   stream {
//       upstream iot_gateway {
//           server 127.0.0.1:1883;
//           server 127.0.0.1:1884;
//           server 127.0.0.1:1885;
//       }
//       server {
//           listen 1883;
//           proxy_pass iot_gateway;
//           proxy_connect_timeout 5s;
//       }
//   }
// ============================================================

#include "net/EventLoop.h"
#include "gateway/GatewayServer.h"
#include "api/ApiServer.h"
#include "common/Logger.h"
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>

static EventLoop* g_mainLoop = nullptr;

void signalHandler(int sig) {
    LOG_INFO("Received signal %d, shutting down...", sig);
    if (g_mainLoop) {
        g_mainLoop->quit();
    }
}

void printUsage(const char* prog) {
    printf("分布式IoT设备监控与指令调度平台 - 网关节点\n\n");
    printf("Usage: %s [options]\n\n", prog);
    printf("Options:\n");
    printf("  -n, --node-id <id>      Gateway node ID (default: auto-generated)\n");
    printf("  -p, --port <port>        MQTT listen port (default: 1883)\n");
    printf("  -a, --api-port <port>    HTTP API port (default: 8080)\n");
    printf("  -t, --threads <num>      Worker threads (default: 4)\n");
    printf("  -r, --redis <host:port>  Redis address (default: 127.0.0.1:6379)\n");
    printf("  -i, --influx <host:port> InfluxDB address (default: 127.0.0.1:8086)\n");
    printf("  -l, --log-level <level>  Log level (default: INFO)\n");
    printf("  -h, --help               Show this help\n");
    printf("\nMulti-node deployment:\n");
    printf("  Start multiple instances with different --node-id and --port,\n");
    printf("  pointing to the same Redis for distributed coordination.\n");
}

int main(int argc, char* argv[]) {
    // ---- 默认参数 ----
    std::string nodeId = "";
    uint16_t mqttPort  = 1883;
    uint16_t apiPort   = 8080;
    int      threadNum = 4;
    RedisConfig redisCfg;
    TsdbConfig  tsdbCfg;
    Logger::setLevel(Logger::INFO);

    // ---- 解析命令行参数 ----
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-n" || arg == "--node-id") && i + 1 < argc) {
            nodeId = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            mqttPort = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if ((arg == "-a" || arg == "--api-port") && i + 1 < argc) {
            apiPort = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if ((arg == "-t" || arg == "--threads") && i + 1 < argc) {
            threadNum = std::atoi(argv[++i]);
        } else if ((arg == "-r" || arg == "--redis") && i + 1 < argc) {
            std::string addr = argv[++i];
            auto colon = addr.find(':');
            if (colon != std::string::npos) {
                redisCfg.host = addr.substr(0, colon);
                redisCfg.port = std::atoi(addr.substr(colon + 1).c_str());
            }
        } else if ((arg == "-i" || arg == "--influx") && i + 1 < argc) {
            std::string addr = argv[++i];
            auto colon = addr.find(':');
            if (colon != std::string::npos) {
                tsdbCfg.host = addr.substr(0, colon);
                tsdbCfg.port = std::atoi(addr.substr(colon + 1).c_str());
            }
        } else if ((arg == "-l" || arg == "--log-level") && i + 1 < argc) {
            std::string level = argv[++i];
            if (level == "TRACE") Logger::setLevel(Logger::TRACE);
            else if (level == "DEBUG") Logger::setLevel(Logger::DEBUG);
            else if (level == "INFO")  Logger::setLevel(Logger::INFO);
            else if (level == "WARN")  Logger::setLevel(Logger::WARN);
            else if (level == "ERROR") Logger::setLevel(Logger::ERROR);
        } else if (arg == "-h" || arg == "--help") {
            printUsage(argv[0]);
            return 0;
        }
    }

    // 自动生成节点 ID
    if (nodeId.empty()) {
        nodeId = ClusterManager::generateNodeId();
    }

    // ---- 安装信号处理 ----
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // ---- 打印配置 ----
    LOG_INFO("========================================");
    LOG_INFO(" 分布式IoT设备监控与指令调度平台");
    LOG_INFO(" Gateway Node: %s", nodeId.c_str());
    LOG_INFO("========================================");
    LOG_INFO(" MQTT port:     %u", mqttPort);
    LOG_INFO(" API port:      %u", apiPort);
    LOG_INFO(" Worker threads: %d", threadNum);
    LOG_INFO(" Redis:         %s:%d", redisCfg.host.c_str(), redisCfg.port);
    LOG_INFO(" InfluxDB:      %s:%d/%s", tsdbCfg.host.c_str(), tsdbCfg.port, tsdbCfg.database.c_str());
    LOG_INFO("========================================");

    // ---- 初始化事件循环 ----
    EventLoop mainLoop;
    g_mainLoop = &mainLoop;

    // ---- 初始化网关 ----
    redisCfg.consumerName = nodeId;  // 消费者名 = 节点 ID
    GatewayServer gateway(&mainLoop, mqttPort, redisCfg, tsdbCfg);
    gateway.setNodeId(nodeId);
    gateway.setThreadNum(threadNum);
    gateway.start();

    // ---- 启动 HTTP API ----
    ApiServer api(&mainLoop, apiPort);
    api.setDeviceCountFn([&gateway]() { return gateway.deviceCount(); });
    api.setCommandSenderFn([&gateway](const std::string& deviceId,
                                       const std::string& command,
                                       const std::string& params) -> int64_t {
        return gateway.enqueueCommand(deviceId, command, params);
    });
    // 注入集群统计回调
    api.setClusterStatsFn([&gateway]() -> std::string {
        auto stats = gateway.clusterStats();
        char buf[512];
        snprintf(buf, sizeof(buf),
            R"({"total_nodes":%zu,"online_nodes":%zu,)"
            R"("total_devices":%zu,"pending_commands":%zu,)"
            R"("local_devices":%zu,"cmd_consumed":%lld})",
            stats.totalNodes, stats.onlineNodes,
            stats.totalDevices, stats.pendingCommands,
            gateway.localDeviceCount(), (long long)gateway.consumedCommandCount());
        return buf;
    });
    api.start();

    // ---- 进入事件循环 (阻塞) ----
    LOG_INFO("Gateway [%s] is running. Press Ctrl+C to stop.", nodeId.c_str());
    mainLoop.loop();

    // ---- 清理 ----
    LOG_INFO("Gateway [%s] shut down gracefully.", nodeId.c_str());
    return 0;
}
