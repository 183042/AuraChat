#include "protocol.h"
#include "ai_manager.h"
#include "config.h"
#include "globals.h"
#include "worker.h"
#include "thread_pool.h"
#include "message.pb.h"
#include "spdlog/spdlog.h"
#include <sstream>
#include <iomanip>
#include <algorithm>

using namespace my_chat;

StreamingAIManager::StreamingAIManager(size_t max_concurrent)
    : max_concurrent_(max_concurrent), running_(true) {
    for (size_t i = 0; i < max_concurrent; ++i) {
        ai_clients_.push_back(std::make_unique<LlamaClient>(Config::AI_BASE_URL()));
    }
    scheduler_thread_ = std::thread([this] { schedule_requests(); });
    spdlog::info("Streaming AI Manager initialized with {} concurrent clients", max_concurrent);
}

// 析构函数中记得 join
StreamingAIManager::~StreamingAIManager() {
    running_ = false;
    cv_.notify_all();
    if (scheduler_thread_.joinable()) scheduler_thread_.join();
}

bool StreamingAIManager::submit_stream_request(int sender_uid,
                          const std::string& prompt,
                     std::shared_ptr<ClientContext> ctx) {
    if (!ctx || !ctx->owner_worker) return false;

    int fd = ctx->fd.load();
    if (fd < 0) return false;

    auto req = std::make_shared<StreamRequest>();
    req->uid = sender_uid;
    req->prompt = prompt;
    req->ctx = ctx;
    req->timestamp = std::chrono::steady_clock::now();
    req->stream_seq = 0;

    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (active_streams_.count(fd) > 0) {
            spdlog::warn("Connection fd {} already has active AI request", fd);
            return false;
        }

        if (pending_requests_.size() > 1000) {
            spdlog::warn("AI request queue full for uid {}", sender_uid);
            return false;
        }

        pending_requests_.push(req);
        spdlog::info("AI request queued: uid={}, fd={}, queue_size={}",
                    sender_uid, fd, pending_requests_.size());
    }
    cv_.notify_one();
    return true;
}

void StreamingAIManager::cancel_request(int uid) {
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto it = active_streams_.begin(); it != active_streams_.end(); ) {
        if (it->second->uid == uid) {
            spdlog::info("Cancelled AI request for uid {} (fd {})", uid, it->first);
            it = active_streams_.erase(it);
        } else {
            ++it;
        }
    }
}

void StreamingAIManager::schedule_requests() {
    while (running_) {
        std::shared_ptr<StreamRequest> req;
        {
            std::unique_lock<std::mutex> lock(mutex_);

            if (pending_requests_.empty() && active_streams_.empty()) {
                cv_.wait_for(lock, std::chrono::milliseconds(100));
                continue;
            }

            if (!pending_requests_.empty() && active_streams_.size() < max_concurrent_) {
                req = pending_requests_.front();
                pending_requests_.pop();

                auto age = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now() - req->timestamp).count();
                if (age > 30) {
                    send_error_response(req, "AI服务排队超时，请稍后再试");
                    continue;
                }

                int fd = req->ctx->fd.load();
                if (fd < 0) continue;
                active_streams_[fd] = req;
                spdlog::info("Starting AI request for fd {}, active now: {}", fd, active_streams_.size());
            } else {
                cv_.wait_for(lock, std::chrono::milliseconds(100));
                continue;
            }
        }

        if (g_workers) {
            g_workers->enqueue([this, req]() {
                process_stream_request(req);
            });
        }
    }
}

void StreamingAIManager::process_stream_request(const std::shared_ptr<StreamRequest>& req) {
    auto ctx = req->ctx;
    int uid = req->uid;

    if (!ctx || ctx->fd.load() < 0) {
        std::unique_lock<std::mutex> lock(mutex_);
        int fd = req->ctx ? req->ctx->fd.load() : -1;
        if (fd >= 0) active_streams_.erase(fd);
        spdlog::info("AI stream skipped: connection closed before start (uid={})", uid);
        return;
    }

    static std::atomic<size_t> rr_counter{0};
    size_t client_idx = rr_counter.fetch_add(1) % ai_clients_.size();
    auto& client = ai_clients_[client_idx];

    const size_t MAX_CHUNK_SIZE = 50;
    const auto FLUSH_INTERVAL = std::chrono::milliseconds(80);
    auto last_flush = std::chrono::steady_clock::now();
    std::string buffer;
    bool success = false;
    bool conn_lost = false;

    try {
        success = client->stream_chat(
            req->prompt,
            [&](const std::string& chunk) {
                buffer += chunk;
                auto now = std::chrono::steady_clock::now();

                if (!ctx || ctx->fd.load() < 0) {
                    conn_lost = true;
                    throw std::runtime_error("Connection closed during AI stream");
                }

                bool has_newline = (chunk.find('\n') != std::string::npos);

                if (buffer.size() >= MAX_CHUNK_SIZE ||
                    now - last_flush >= FLUSH_INTERVAL ||
                    has_newline) {

                    if (!buffer.empty()) {
                        send_stream_chunk(uid, ctx, buffer, false, false, req->stream_seq++);
                        buffer.clear();
                        last_flush = now;
                        std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    }
                }
            },
            [&]() {
                if (!ctx || ctx->fd.load() < 0) {
                    conn_lost = true;
                    return;
                }
                send_stream_chunk(uid, ctx, buffer, false, true, req->stream_seq++);
                buffer.clear();
            }
        );
    } catch (const std::exception& e) {
        if (conn_lost)
            spdlog::info("AI stream cancelled: connection closed (uid={})", uid);
        else {
            spdlog::error("Stream processing error for uid {}: {}", uid, e.what());
            success = false;
        }
    }

    if (!success && !conn_lost) {
        std::string fallback = generate_fallback_response(req->prompt);
        for (size_t i = 0; i < fallback.size(); i += 15) {
            std::string chunk = fallback.substr(i, 15);
            bool last = (i + 15 >= fallback.size());
            if (ctx && ctx->fd.load() >= 0)
                send_stream_chunk(uid, ctx, chunk, false, last, req->stream_seq++);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);
        int fd = req->ctx ? req->ctx->fd.load() : -1;
        if (fd >= 0) active_streams_.erase(fd);
    }
}

void StreamingAIManager::send_stream_chunk(int uid,
                  std::shared_ptr<ClientContext> ctx,
                  const std::string& chunk,
                  bool is_first,
                  bool is_last,
                  int32_t stream_seq) {
    if (!ctx || !ctx->owner_worker) return;
    if (chunk.empty() && !is_last) return;

    ChatMessage msg;
    msg.set_from_uid(9999);
    msg.set_to_uid(uid);
    msg.set_content(chunk);
    msg.set_is_stream(!is_last || !chunk.empty());
    msg.set_is_last_chunk(is_last);
    msg.set_stream_seq(stream_seq);
    msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    std::string out;
    if (!msg.SerializeToString(&out)) {
        spdlog::error("Failed to serialize stream message for uid {}", uid);
        return;
    }

    auto worker = ctx->owner_worker;
    worker->queue_send_packet(ctx, static_cast<uint32_t>(CmdType::CHAT), 0, out);
}

void StreamingAIManager::send_error_response(std::shared_ptr<StreamRequest> req,
                        const std::string& error_msg) {
    if (req && req->ctx) {
        send_stream_chunk(req->uid, req->ctx, error_msg, false, true, req->stream_seq++);
    }
}

std::string StreamingAIManager::generate_fallback_response(const std::string& prompt) {
    std::string lower = prompt;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower.find("你好") != std::string::npos || lower.find("hi") != std::string::npos || lower == "hello")
        return "你好！我是 AI 助手。虽然目前没有连接到外部 AI 服务，但我可以帮你解答一些基本问题。\n\n你可以尝试：\n- 问我关于时间、日期的问题\n- 跟我聊天\n- 或者配置 AI_BASE_URL 环境变量接入真正的 AI 服务";

    if (lower.find("帮助") != std::string::npos || lower.find("help") != std::string::npos)
        return "这是 WeChat 内置 AI 助手。\n\n当前运行在离线模式，要接入真正的 AI 服务，请设置环境变量：\n  export AI_BASE_URL=http://your-llm-server:port\n\n支持的 API 格式：OpenAI-compatible /v1/chat/completions";

    if (lower.find("时间") != std::string::npos || lower.find("几点") != std::string::npos) {
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << "现在是 " << std::put_time(std::localtime(&t), "%Y年%m月%d日 %H:%M:%S");
        return ss.str();
    }

    if (lower.find("天气") != std::string::npos)
        return "抱歉，我目前无法获取实时天气数据。要获得完整的 AI 功能，请配置 AI_BASE_URL 环境变量连接到外部 AI 服务。";

    if (lower.find("你是谁") != std::string::npos || lower.find("你是什么") != std::string::npos)
        return "我是 WeChat 内置的 AI 助手。当前运行在离线模式，只能提供基础的回答。\n\n要获得更强大的 AI 能力，可以连接 OpenAI 兼容的 API 服务，例如：\n- Ollama\n- llama.cpp server\n- vLLM\n- 或其他兼容 /v1/chat/completions 的服务";

    if (prompt.length() < 5)
        return "你好！有什么我可以帮你的吗？\n\n提示：当前 AI 运行在离线模式，回答能力有限。请设置 AI_BASE_URL 接入真正的 AI 服务。";

    return "收到你的消息：「" + prompt.substr(0, 100) + (prompt.length() > 100 ? "..." : "") + "」\n\n当前 AI 服务未连接，我正在离线模式下工作。\n\n要获得智能回复，请配置 AI_BASE_URL 环境变量：\n  export AI_BASE_URL=http://your-llm-server:port\n\n然后重启服务器即可。";
}
