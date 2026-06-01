#pragma once
#include "event_loop.h"
#include <functional>
#include <string>

class Acceptor {
public:
    Acceptor(EventLoop* loop, uint16_t port);
    void handle_accept(std::function<void(int, std::string, uint16_t)> cb);
private:
    EventLoop* loop_;
    int listen_fd_;
};
