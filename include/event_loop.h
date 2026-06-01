#pragma once
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <vector>
#include <mutex>
#include <atomic>
#include <functional>

class EventLoop {
public:
    EventLoop();
    ~EventLoop();
    void loop();
    void stop() { running_ = false; wakeup(); }
    void queue_in_loop(std::function<void()> cb);
    void update_event(int fd, uint32_t events, void* ptr);
    void remove_event(int fd);
    int epfd() const { return epfd_; }
    void set_timeout_getter(std::function<int()> g) { timeout_getter_ = std::move(g); }
    void set_on_loop_once(std::function<void()> cb) { on_loop_once_ = std::move(cb); }
private:
    void wakeup();
    void handle_wakeup();
    void do_pending_functors();
    int epfd_;
    int wakeup_fd_;
    std::atomic<bool> running_{true};
    std::vector<struct epoll_event> events_;
    std::mutex mutex_;
    std::vector<std::function<void()>> pending_functors_;
    std::function<int()> timeout_getter_;
    std::function<void()> on_loop_once_;
};
