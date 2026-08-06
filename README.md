# 分布式IoT设备监控与指令调度平台

基于 C++17 + epoll 多 Reactor 模型的**分布式** IoT 设备管理平台，支持**水平扩展**、多网关节点通过 **Redis Streams 消费者组**协同消费指令，通过 **Nginx 四层负载均衡**分发设备连接。

## 核心分布式特性

| 特性 | 技术实现 |
|---|---|
| **水平扩展** | 多网关节点, 接入同一 Redis, 自动负载均衡 |
| **消费者组协同** | Redis Streams XREADGROUP + XACK, 多节点竞争消费 |
| **L4 负载均衡** | Nginx stream proxy, TCP 透明分发设备连接 |
| **分布式设备注册表** | Redis Hash: `iot:devices` (deviceId → gatewayNode) |
| **跨节点指令路由** | Redis Pub/Sub: `iot:route:{nodeId}` |
| **故障检测** | TTL 心跳 + 定期扫描, 超时节点自动剔除 |
| **故障转移** | XCLAIM 认领故障节点未 ACK 消息 |

## 快速开始

```bash
# 1. 启动 Redis (分布式协调)
redis-server &

# 2. 编译
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)

# 3. 启动多个网关节点 (分布式)
./gateway_server --node-id gateway-1 --port 1883 --api-port 8081 --redis 127.0.0.1:6379 &
./gateway_server --node-id gateway-2 --port 1884 --api-port 8082 --redis 127.0.0.1:6379 &
./gateway_server --node-id gateway-3 --port 1885 --api-port 8083 --redis 127.0.0.1:6379 &

# 4. Nginx L4 负载均衡 (设备统一入口 :1883)
cp ../config/nginx.conf /etc/nginx/ && nginx -s reload

# 5. 启动设备模拟器 (压测)
./device_simulator --host 127.0.0.1 --port 1883 --devices 10000 --interval 5

# 6. 下发指令
curl -X POST http://127.0.0.1:8081/api/v1/commands \
  -H "Content-Type: application/json" \
  -d '{"device_id":"sim-device-00001","command":"reboot","params":{"delay":5}}'

# 7. 查看集群状态
curl http://127.0.0.1:8081/api/v1/cluster

# 8. Docker 一键部署
cd docker && docker-compose up -d
```

## 系统架构

```
                    ┌──────────────────────────┐
                    │    Nginx L4 负载均衡      │
                    │    listen :1883 (TCP)     │
                    └──────────┬───────────────┘
               ┌───────────────┼───────────────┐
               │               │               │
        ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
        │ Gateway-1   │ │ Gateway-2  │ │ Gateway-3  │
        │ :1883       │ │ :1884      │ │ :1885      │
        └──────┬──────┘ └─────┬──────┘ └─────┬──────┘
               │               │               │
               └───────────────┼───────────────┘
                               │
             ┌─────────────────┼─────────────────┐
             │                 │                 │
      ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
      │    Redis    │  │  InfluxDB   │  │   Dashboard  │
      │ Streams     │  │  时序存储    │  │  Web 看板    │
      │ 设备注册表   │  │             │  │             │
      │ Pub/Sub     │  │             │  │             │
      └─────────────┘  └─────────────┘  └─────────────┘
```

## API 接口

| Method | Path | 说明 |
|---|---|---|
| POST | `/api/v1/commands` | 下发指令 → Redis Streams (XADD) |
| GET | `/api/v1/devices` | 在线设备数 (分布式注册表) |
| GET | `/api/v1/cluster` | 集群拓扑信息 |
| GET | `/api/v1/nodes` | 集群节点列表 |
| GET | `/api/v1/device/:id` | 设备详情 + 遥测数据 |
| GET | `/api/v1/commands/:id` | 指令执行状态 |
| GET | `/api/v1/health` | 健康检查 |

## 技术栈

- **网络框架**: epoll 多 Reactor (One loop per thread)
- **应用协议**: MQTT 3.1.1
- **分布式协调**: Redis Streams (消费者组 + XACK) + Pub/Sub + Hash
- **指令队列**: Redis Streams (XADD/XREADGROUP/XACK/XCLAIM)
- **时序存储**: InfluxDB (Line Protocol, 批量异步写入)
- **负载均衡**: Nginx Stream (TCP L4)
- **语言**: C++17 (RAII, 智能指针, lambda, move 语义)

## 项目结构

```
分布式项目/
├── config/
│   ├── gateway.yaml         # 网关配置 (v2.0 集群配置)
│   └── nginx.conf           # Nginx L4 负载均衡配置
├── docker/
│   ├── docker-compose.yml   # 一键部署完整分布式环境
│   └── Dockerfile.gateway   # 网关节点 Docker 镜像
├── src/
│   ├── common/              # 公共组件
│   ├── net/                 # epoll 多 Reactor 网络框架
│   ├── protocol/            # MQTT 3.1.1 协议编解码
│   ├── gateway/             # ★ 网关核心 + 集群管理
│   │   ├── GatewayServer    # 分布式网关 (消费者组 + 跨节点路由)
│   │   ├── ClusterManager   # 集群协调 (节点发现/心跳/故障转移)
│   │   └── DeviceManager    # 分布式设备注册表 (Redis 同步)
│   ├── storage/             # Redis Streams + InfluxDB
│   ├── api/                 # REST API Server
│   └── platform/            # ★ 管理平台 (v2.0 新增)
│       ├── WebDashboard     # Web 设备监控看板
│       ├── AlertEngine      # 告警规则引擎
│       └── MetricsCollector # 指标采集器
├── apps/
│   ├── gateway_main.cpp     # 网关节点启动入口
│   └── simulator_main.cpp   # 设备模拟器 (压测)
└── test/
    └── echo_test.cpp
```

## License

MIT
