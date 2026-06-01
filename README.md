# AuraChat 🚀

AuraChat 是一款基于 **C++17** 开发的高性能即时通讯（IM）服务端。项目采用 **Reactor 模式** 与 **Epoll 多路复用技术**，并深度集成了 **大语言模型（LLM）流式对话** 接口，具备高并发、低延迟以及完备的存储兜底机制。

---

## 🌟 核心特性

1. **高性能并发反应堆 (Reactor)**
   - 采用多工作线程（Multi-Worker）事件循环架构，每个 Worker 独立管理 Epoll 实例与定时器。
   - 利用自定义对象池（ContextPool）高效复用连接上下文，极大地减少了高并发场景下频繁内存分配带来的开销。
   - 使用内置线程池（ThreadPool）异步执行密集的业务逻辑，避免阻塞网络 I/O 线程。

2. **流式 AI 对话集成**
   - 深度集成 OpenAI 兼容接口（支持 `/v1/chat/completions`，如 Ollama, llama.cpp, vLLM 等服务）。
   - 引入并发请求队列和自适应调度机制（StreamingAIManager），控制 AI 服务并发压力。
   - 具备**离线 AI 兜底逻辑**，即使外部大模型服务不可用，也能自动降级提供本地基础服务。

3. **双重存储防线 (MySQL & JSON Fallback)**
   - 采用批量异步写入机制（DBBatchWriter）提升数据库并发写入吞吐量。
   - 具备**无缝本地存储退化能力**，在 MySQL 数据库未就绪或连接丢失时，系统会自动切换至本地高效的 JSON 文件存储，保证系统关键服务不中断。

4. **完备的社交及会话管理**
   - 实现了分段锁（Shard Buckets）高并发安全的 Session 绑定与解绑机制。
   - 支持完整的用户注册、登录校验、离线消息投递、好友请求及检索功能。
   - 内置私聊流量控制（Rate Limiting）与心跳保活定时器（Timer）。

---

## 📂 项目结构

```text
AuraChat/
├── CMakeLists.txt             # 构建配置文件
├── message.proto              # Protobuf 协议定义文件
├── include/                   # 头文件目录
│   ├── config.h               # 环境变量配置
│   ├── protocol.h             # 网络协议与命令定义
│   ├── thread_pool.h          # 任务线程池
│   ├── timer.h                # 时间轮/定时器设计
│   ├── event_loop.h           # Epoll Reactor 循环
│   ├── context.h              # 客户端上下文与对象池
│   ├── connection.h           # 封装的 TCP 连接
│   ├── worker.h               # 事件驱动 Worker
│   ├── acceptor.h             # 端口接收器
│   ├── session_manager.h      # 线程安全会话管理
│   ├── db_writer.h            # 数据库及 JSON 双模存储
│   ├── llama_client.h         # 流式 HTTP 客户端
│   ├── ai_manager.h           # AI 队列管理与流调度
│   ├── business.h             # 业务逻辑处理器
│   └── globals.h              # 全局变量定义
└── src/                       # 源文件目录
    ├── event_loop.cpp
    ├── connection.cpp
    ├── worker.cpp
    ├── acceptor.cpp
    ├── db_writer.cpp
    ├── ai_manager.cpp
    ├── business.cpp
    └── main.cpp

1. 环境准备
项目运行需要具备以下环境：
Linux 操作系统（如 Ubuntu 20.04+）
支持 C++17 的编译器（GCC 8+ 或 Clang 7+）
CMake 3.12+
第三方库依赖：
Protobuf：用于数据序列化。
libcurl：用于请求 AI 流式接口。
mysqlclient：用于 MySQL 数据持久化。
nlohmann_json：用于本地兜底文件读写。
spdlog：高效异步日志库。
下载llama.cpp
下载开源大模型，如Qwen_Qwen3-8B-Q4_K_M.gguf


sudo apt-get update
sudo apt-get install build-essential cmake libprotobuf-dev protobuf-compiler libcurl4-openssl-dev libmysqlclient-dev libspdlog-dev nlohmann-json3-dev

2. 编译构建
AuraChat 使用 CMake 自动构建，并且会在编译期间自动通过 protobuf_generate_cpp 编译您的 message.proto 文件：

# 创建并进入构建目录
mkdir build && cd build

# 生成 Makefile
cmake ..

# 开始编译
make

支持的命令类型 (CmdType)
LOGIN (1): 用户登录校验
CHAT (2): 聊天消息转发与存储（发往 9999 触发大语言模型流式对话）
ACK (3): 通用业务回执
HEARTBEAT (4): 心跳保活探测
REGISTER (5): 新用户注册
ADD_FRIEND (6): 发送好友申请
ACCEPT_FRIEND (7): 接受好友申请
FRIEND_LIST (8): 获取在线/离线好友列表
SEARCH_USER (9): 用户模糊查询
FRIEND_REQUESTS (10): 获取待处理的好友申请列表

这个是一个服务器，还需要依赖llama.cpp和下载好的本地大模型进行cd ~/llama.cpp

./build/bin/llama-server \
    -m ~/Qwen_Qwen3-8B-Q4_K_M.gguf \
    --host 0.0.0.0 \
    --port 8082 \
    -c 32768 \
    -ngl 0 \
    --threads 4
这个是一个例子，具体的目录在各自的电脑上不同，当然这里threads用4,内存比较大的可以增加数量，有gpu的更好。
首先要通过llama.cpp来启动和下载好的本地模型进行连接，然后启动服务器和llama.cpp进行一个连接，最后启动客户端发送请求。

