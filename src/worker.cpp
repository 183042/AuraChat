#include "worker.h"
#include "connection.h"
#include "config.h"
#include "protocol.h"
#include "spdlog/spdlog.h"

// ContextPool 的延迟定义与实现
ContextPool::ContextPool(size_t size, Worker* owner) : owner_(owner) {
    storage_.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        auto ctx = std::make_shared<ClientContext>();
        ctx->owner_worker = owner_;
        ctx->pool_idx = static_cast<int>(i);
        storage_.push_back(ctx);
        free_list_.push_back(i);
    }
}

std::shared_ptr<ClientContext> ContextPool::acquire() {
    if (free_list_.empty()) return std::make_shared<ClientContext>();
    int idx = free_list_.back();
    free_list_.pop_back();
    return storage_[idx];
}

void ContextPool::release(std::shared_ptr<ClientContext> ctx) {
    if (!ctx || ctx->pool_idx == -1) return;
    ctx->reset();
    free_list_.push_back(ctx->pool_idx);
}

// Worker 实现
Worker::Worker() {
    pool_ = std::make_unique<ContextPool>(Config::POOL_SIZE(), this);
    loop_.set_timeout_getter([this]{ return timer_.TimeToSleep(); });
    loop_.set_on_loop_once([this]{ while(timer_.CheckTimer()); });
    thread_ = std::thread([this]{ loop_.loop(); });
}

Worker::~Worker() {
    loop_.stop();
    if(thread_.joinable()) thread_.join();
}

void Worker::queue_add_connection(int fd, const std::string& ip, uint16_t p) {
    loop_.queue_in_loop([this, fd, ip, p]{ add_connection_in_loop(fd, ip, p); });
}

void Worker::add_connection_in_loop(int fd, const std::string& ip, uint16_t port) {
    auto conn = std::make_shared<TcpConnection>(this, fd, ip, port);
    connections_[fd] = conn;
    conn->start();
    spdlog::info("New connection from {}:{}", ip, port);
}

void Worker::queue_send_packet(std::shared_ptr<ClientContext> ctx, uint32_t c, uint32_t s, const std::string& b) {
    loop_.queue_in_loop([this, ctx, c, s, b]{ send_packet_in_loop(ctx, c, s, b); });
}

void Worker::send_packet_in_loop(std::shared_ptr<ClientContext> ctx, uint32_t cmd, uint32_t seq, const std::string& body) {
    int fd = ctx->fd.load();
    if (fd < 0) return;
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    PacketHeader hdr{Config::PROTO_MAGIC, (uint32_t)body.size(), cmd, seq};
    std::string pkg;
    pkg.append((char*)&hdr, sizeof(hdr));
    pkg.append(body);
    it->second->send(pkg);
}

void Worker::force_close_in_loop(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;
    auto conn = it->second;
    connections_.erase(it);
    conn->do_force_close();
}

void Worker::queue_close_connection(int fd) {
    loop_.queue_in_loop([this, fd]{ force_close_in_loop(fd); });
}

void Worker::start_heartbeat_timer(int fd) {
    cancel_heartbeat_timer(fd);
    int64_t ms = (int64_t)Config::HEARTBEAT_SEC() * 1000;
    heartbeat_timers_[fd] = timer_.AddTimer(ms, [this, fd](const TimerNode&){
        force_close_in_loop(fd);
    });
}

void Worker::cancel_heartbeat_timer(int fd) {
    auto it = heartbeat_timers_.find(fd);
    if(it != heartbeat_timers_.end()){
        timer_.DelTimer(it->second);
        heartbeat_timers_.erase(it);
    }
}
