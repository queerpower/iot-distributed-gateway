cmake_minimum_required(VERSION 3.14)
project(IoTDistributedPlatform VERSION 2.0.0 LANGUAGES CXX)

# C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Project: 分布式IoT设备监控与指令调度平台 v2.0")

# ============================================================
# 第三方依赖查找 (优先从 F:\third_party_library 查找)
# ============================================================
set(THIRD_PARTY_DIR "F:/third_party_library"
    CACHE PATH "第三方库存放目录")

# ----- hiredis (Redis C 客户端) -----
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h
    PATHS ${THIRD_PARTY_DIR}/hiredis/include
    DOC "hiredis include directory")

find_library(HIREDIS_LIBRARY hiredis
    PATHS ${THIRD_PARTY_DIR}/hiredis/lib
    DOC "hiredis library")

if(HIREDIS_INCLUDE_DIR AND HIREDIS_LIBRARY)
    message(STATUS "Found hiredis: ${HIREDIS_LIBRARY}")
    set(HIREDIS_FOUND TRUE)
else()
    message(FATAL_ERROR "hiredis not found — Redis is now a required dependency. "
                        "Install libhiredis or set HIREDIS_INCLUDE_DIR/HIREDIS_LIBRARY.")
endif()

# ============================================================
# 公共头文件 (header-only)
# ============================================================
set(COMMON_HEADERS
    src/common/NonCopyable.h
    src/common/Logger.h
    src/common/Timestamp.h)

# ============================================================
# 网络库 (core net library)
# ============================================================
set(NET_SOURCES
    src/net/Buffer.cpp
    src/net/Channel.cpp
    src/net/EventLoop.cpp
    src/net/EventLoopThread.cpp
    src/net/EventLoopThreadPool.cpp
    src/net/TcpConnection.cpp
    src/net/TcpServer.cpp
    src/net/Acceptor.cpp
    src/net/Socket.cpp
    src/net/InetAddress.cpp)

set(NET_HEADERS
    src/net/Callbacks.h
    src/net/Buffer.h
    src/net/Channel.h
    src/net/EventLoop.h
    src/net/EventLoopThread.h
    src/net/EventLoopThreadPool.h
    src/net/TcpConnection.h
    src/net/TcpServer.h
    src/net/Acceptor.h
    src/net/Socket.h
    src/net/InetAddress.h)

add_library(iot_net STATIC ${NET_SOURCES} ${NET_HEADERS} ${COMMON_HEADERS})
target_include_directories(iot_net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(iot_net PUBLIC pthread)

# ============================================================
# 协议库 (MQTT codec)
# ============================================================
set(PROTOCOL_SOURCES
    src/protocol/MqttCodec.cpp)

set(PROTOCOL_HEADERS
    src/protocol/MqttTypes.h
    src/protocol/MqttCodec.h)

add_library(iot_protocol STATIC ${PROTOCOL_SOURCES} ${PROTOCOL_HEADERS})
target_include_directories(iot_protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(iot_protocol PUBLIC iot_net)

# ============================================================
# 存储库 (Redis + TSDB)  — v2.0: 完整 Redis Streams 消费者组
# ============================================================
set(STORAGE_SOURCES
    src/storage/RedisClient.cpp
    src/storage/TsdbClient.cpp)

set(STORAGE_HEADERS
    src/storage/RedisClient.h
    src/storage/TsdbClient.h)

add_library(iot_storage STATIC ${STORAGE_SOURCES} ${STORAGE_HEADERS})
target_include_directories(iot_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(iot_storage PUBLIC iot_net)

target_include_directories(iot_storage PRIVATE ${HIREDIS_INCLUDE_DIR})
target_link_libraries(iot_storage PRIVATE ${HIREDIS_LIBRARY})

# ============================================================
# 网关业务库 v2.0 — Gateway + API + ClusterManager + DeviceManager
# ============================================================
set(GATEWAY_SOURCES
    src/gateway/DeviceManager.cpp
    src/gateway/GatewayServer.cpp
    src/gateway/ClusterManager.cpp
    src/api/ApiServer.cpp)

set(GATEWAY_HEADERS
    src/gateway/DeviceManager.h
    src/gateway/GatewayServer.h
    src/gateway/ClusterManager.h
    src/api/ApiServer.h)

add_library(iot_gateway STATIC ${GATEWAY_SOURCES} ${GATEWAY_HEADERS})
target_include_directories(iot_gateway PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(iot_gateway PUBLIC iot_net iot_protocol iot_storage)

# ============================================================
# 管理平台库 v2.0 — Dashboard + AlertEngine + MetricsCollector
# ============================================================
set(PLATFORM_SOURCES
    src/platform/AlertEngine.cpp
    src/platform/MetricsCollector.cpp
    src/platform/WebDashboard.cpp)

set(PLATFORM_HEADERS
    src/platform/AlertEngine.h
    src/platform/MetricsCollector.h
    src/platform/WebDashboard.h)

add_library(iot_platform STATIC ${PLATFORM_SOURCES} ${PLATFORM_HEADERS})
target_include_directories(iot_platform PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(iot_platform PUBLIC iot_net iot_storage)

# ============================================================
# 可执行文件
# ============================================================

# --- 网关主程序 (分布式多节点) ---
add_executable(gateway_server apps/gateway_main.cpp)
target_link_libraries(gateway_server PRIVATE iot_gateway iot_storage iot_protocol iot_net)

# --- 设备模拟器 (压测工具) ---
add_executable(device_simulator apps/simulator_main.cpp)
target_link_libraries(device_simulator PRIVATE iot_protocol iot_net)

# ============================================================
# 测试
# ============================================================
option(BUILD_TESTS "Build test targets" ON)
if(BUILD_TESTS)
    add_executable(echo_test test/echo_test.cpp)
    target_link_libraries(echo_test PRIVATE iot_net)
endif()

# ============================================================
# 安装
# ============================================================
install(TARGETS gateway_server device_simulator
    RUNTIME DESTINATION bin)

install(FILES config/gateway.yaml
    DESTINATION etc/iot-gateway)

install(FILES config/nginx.conf
    DESTINATION etc/iot-gateway)
