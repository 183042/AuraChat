#pragma once
#include <memory>
#include <string>

class Worker;
struct ClientContext;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(Worker* owner, int fd, const std::string& ip, uint16_t port);
    ~TcpConnection();
    void start();
    void handle_events(uint32_t revents);
    void send(const std::string& data);
    void do_force_close();
    std::shared_ptr<ClientContext> ctx() const { return ctx_; }
private:
    void handle_read();
    void handle_write();
    void close_impl();
    Worker* owner_;
    int fd_;
    std::string ip_;
    uint16_t port_;
    std::shared_ptr<ClientContext> ctx_;
    bool closed_{false};
};
