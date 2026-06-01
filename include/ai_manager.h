#pragma once
#include "context.h"
#include "llama_client.h"
#include <queue>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <memory>

class StreamingAIManager {
public:
    StreamingAIManager(size_t max_concurrent = 20);
    ~StreamingAIManager();

    bool submit_stream_request(int sender_uid, const std::string& prompt, std::shared_ptr<ClientContext> ctx);
    void cancel_request(int uid);

private:
    struct StreamRequest {
        int uid;
        std::string prompt;
        std::shared_ptr<ClientContext> ctx;
        std::chrono::steady_clock::time_point timestamp;
        int stream_seq;
    };

    void schedule_requests();
    void process_stream_request(const std::shared_ptr<StreamRequest>& req);
    void send_stream_chunk(int uid, std::shared_ptr<ClientContext> ctx, const std::string& chunk, bool is_first, bool is_last, int32_t stream_seq);
    void send_error_response(std::shared_ptr<StreamRequest> req, const std::string& error_msg);
    std::string generate_fallback_response(const std::string& prompt);

    std::queue<std::shared_ptr<StreamRequest>> pending_requests_;
    std::unordered_map<int, std::shared_ptr<StreamRequest>> active_streams_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_;
    std::thread scheduler_thread_;
    std::vector<std::unique_ptr<LlamaClient>> ai_clients_;
    size_t max_concurrent_;
};
