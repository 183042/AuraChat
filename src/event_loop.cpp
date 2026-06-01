#include "event_loop.h"
#include "spdlog/spdlog.h"
#include <cstring>
#include <cerrno>

inline int create_eventfd() {
    int efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) {
        spdlog::critical("eventfd failed: {}", strerror(errno));
        exit(1);
    }
    return efd;
}

EventLoop::EventLoop() {
    epfd_ = epoll_create1(EPOLL_CLOEXEC);
    wakeup_fd_ = create_eventfd();
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = this;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, wakeup_fd_, &ev);
    events_.resize(1024);
}

EventLoop::~EventLoop() {
    stop();
    close(wakeup_fd_);
    close(epfd_);
}

#include"connection.h"

void EventLoop::loop() {
    while (running_) {
        int timeout = timeout_getter_ ? timeout_getter_() : 1000;
        int n = epoll_wait(epfd_, events_.data(), static_cast<int>(events_.size()), timeout);
        for (int i = 0; i < n; ++i) {
            auto* ptr = events_[i].data.ptr;
            if (ptr == this) { handle_wakeup(); continue; }
            auto* conn = static_cast<TcpConnection*>(ptr);
            conn->handle_events(events_[i].events);
        }
        if (on_loop_once_) on_loop_once_();
        do_pending_functors();
    }
}

void EventLoop::queue_in_loop(std::function<void()> cb) {
    { std::lock_guard<std::mutex> l(mutex_); pending_functors_.push_back(std::move(cb)); }
    wakeup();
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    write(wakeup_fd_, &one, sizeof(one));
}

void EventLoop::handle_wakeup() {
    uint64_t val;
    read(wakeup_fd_, &val, sizeof(val));
}

void EventLoop::do_pending_functors() {
    std::vector<std::function<void()>> fs;
    { std::lock_guard<std::mutex> l(mutex_); fs.swap(pending_functors_); }
    for(auto& f : fs) f();
}

void EventLoop::update_event(int fd, uint32_t evs, void* p) {
    struct epoll_event ev;
    ev.events = evs;
    ev.data.ptr = p;
    epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
}

void EventLoop::remove_event(int fd) {
    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
}
