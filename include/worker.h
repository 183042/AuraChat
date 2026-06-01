#pragma once
#include "event_loop.h"
#include "context.h"
#include "timer.h"
#include <unordered_map>
#include <memory>
#include <thread>

class TcpConnection;

class Worker {
public:
    Worker();
    ~Worker();
    void queue_add_connection(int fd, const std::string& ip, uint16_t port);
    void queue_send_packet(std::shared_ptr<ClientContext> ctx, uint32_t cmd, uint32_t seq, const std::string& body);
    void queue_close_connection(int fd);
    EventLoop* loop() { return &loop_; }
    ContextPool* pool() const { return pool_.get(); }
    void start_heartbeat_timer(int fd);
    void cancel_heartbeat_timer(int fd);
    void reset_heartbeat_timer(int fd) { start_heartbeat_timer(fd); }
private:
    friend class TcpConnection;
    void add_connection_in_loop(int fd, const std::string& ip, uint16_t port);
    void send_packet_in_loop(std::shared_ptr<ClientContext> ctx, uint32_t cmd, uint32_t seq, const std::string& body);
    void force_close_in_loop(int fd);
    EventLoop loop_;
    std::thread thread_;
    std::unique_ptr<ContextPool> pool_;
    std::unordered_map<int, std::shared_ptr<TcpConnection>> connections_;
    Timer timer_;
    std::unordered_map<int, TimerNodeBase> heartbeat_timers_;
};
