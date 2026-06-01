#include "acceptor.h"
#include "config.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

Acceptor::Acceptor(EventLoop* loop, uint16_t port) : loop_(loop) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(listen_fd_, Config::BACKLOG());

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = this;
    epoll_ctl(loop_->epfd(), EPOLL_CTL_ADD, listen_fd_, &ev);
}

void Acceptor::handle_accept(std::function<void(int, std::string, uint16_t)> cb) {
    while (true) {
        sockaddr_in peer;
        socklen_t len = sizeof(peer);
        int fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) break;

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        cb(fd, ip, ntohs(peer.sin_port));
    }
}
