#include <iostream>
#include <vector>
#include <memory>
#include <atomic>
#include <csignal>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include <curl/curl.h>

#include"globals.h"
#include "config.h"
#include "thread_pool.h"
#include "db_writer.h"
#include "ai_manager.h"
#include "event_loop.h"
#include "acceptor.h"
#include "worker.h"

// 全局变量定义
std::unique_ptr<ThreadPool> g_workers;
std::unique_ptr<DBBatchWriter> g_db_writer;
std::unique_ptr<StreamingAIManager> g_streaming_ai;
std::atomic<int> g_conn_count{0};
std::atomic<bool> g_running{true};

int main() {
    curl_global_init(CURL_GLOBAL_ALL);

    try {
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            "logs/im_server.log", 1024 * 1024 * 500, 5);
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        std::vector<spdlog::sink_ptr> sinks{file_sink, console_sink};
        auto logger = std::make_shared<spdlog::logger>("im", sinks.begin(), sinks.end());
        spdlog::set_default_logger(logger);
        spdlog::set_level(spdlog::level::info);
        spdlog::flush_on(spdlog::level::info);
        spdlog::flush_every(std::chrono::seconds(3));
    } catch (const spdlog::spdlog_ex &ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
        return 1;
    }

    spdlog::info("========================================");
    spdlog::info("IM Server with Streaming AI starting...");
    spdlog::info("========================================");

    int worker_num = Config::WORKER_NUM();
    spdlog::info("Starting with {} workers", worker_num);

    g_workers = std::make_unique<ThreadPool>(worker_num * 2);
    g_db_writer = std::make_unique<DBBatchWriter>();
    g_streaming_ai = std::make_unique<StreamingAIManager>(Config::AI_MAX_CONCURRENT());

    spdlog::info("Streaming AI Manager ready (max concurrent: {})", Config::AI_MAX_CONCURRENT());
    spdlog::info("AI Service URL: {}", Config::AI_BASE_URL());

    std::vector<std::unique_ptr<Worker>> workers;
    for (int i = 0; i < worker_num; ++i) {
        workers.push_back(std::make_unique<Worker>());
        spdlog::info("Worker {} started", i);
    }

    EventLoop main_loop;
    Acceptor acceptor(&main_loop, Config::PORT());
    uint32_t rr = 0;

    spdlog::info("IM Server listening on port {}", Config::PORT());
    spdlog::info("Server ready! Waiting for connections...");

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, [](int){ g_running = false; });
    signal(SIGTERM, [](int){ g_running = false; });

    std::vector<struct epoll_event> accept_events(64);
    while (g_running) {
        int n = epoll_wait(main_loop.epfd(), accept_events.data(), accept_events.size(), 100);
        for (int i = 0; i < n; ++i) {
            acceptor.handle_accept([&](int fd, std::string ip, uint16_t port){
                workers[rr++ % worker_num]->queue_add_connection(fd, ip, port);
            });
        }
    }

    spdlog::info("Shutting down...");

    workers.clear();
    spdlog::info("All workers stopped");

    g_streaming_ai.reset();
    spdlog::info("Streaming AI Manager stopped");

    g_db_writer.reset();
    spdlog::info("DB Batch Writer stopped");

    g_workers.reset();
    spdlog::info("Thread pool stopped");

    curl_global_cleanup();
    spdlog::info("Server shutdown complete");
    return 0;
}
