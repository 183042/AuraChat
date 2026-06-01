#include "connection.h"
#include "worker.h"
#include "context.h"
#include "protocol.h"
#include "config.h"
#include "globals.h"
#include "session_manager.h"
#include "ai_manager.h"
#include "business.h"
#include "thread_pool.h"
#include <unistd.h>

TcpConnection::TcpConnection(Worker* owner, int fd, const std::string& ip, uint16_t port)
    : owner_(owner), fd_(fd), ip_(ip), port_(port) {
    ctx_ = owner_->pool()->acquire();
    ctx_->fd = fd;
    ctx_->update_time();
    set_keepalive(fd);
    g_conn_count++;
}

TcpConnection::~TcpConnection() {
    if (fd_ >= 0) ::close(fd_);
    if (!closed_) g_conn_count--;
}

void TcpConnection::start() {
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    ev.data.ptr = this;
    epoll_ctl(owner_->loop()->epfd(), EPOLL_CTL_ADD, fd_, &ev);
    owner_->start_heartbeat_timer(fd_);
}

void TcpConnection::handle_events(uint32_t revents) {
    if (revents & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
        owner_->force_close_in_loop(fd_);
        return;
    }
    if (revents & EPOLLIN) handle_read();
    if (revents & EPOLLOUT) handle_write();
}

void TcpConnection::handle_read() {
    char buf[8192];
    while(true) {
        ssize_t n = read(fd_, buf, sizeof(buf));
        if(n > 0) {
            ctx_->recv_buf.append(buf, n);
            ctx_->update_time();
            owner_->reset_heartbeat_timer(fd_);
        } else if(n == 0 || (n < 0 && errno != EAGAIN)) {
            owner_->force_close_in_loop(fd_);
            return;
        } else break;
    }

    while(ctx_->recv_buf.size() >= sizeof(PacketHeader)) {
        auto* hdr = (PacketHeader*)ctx_->recv_buf.data();
        if(hdr->magic != Config::PROTO_MAGIC || hdr->body_len > Config::MAX_BODY_LEN) {
            owner_->force_close_in_loop(fd_);
            return;
        }
        if(ctx_->recv_buf.size() < sizeof(PacketHeader) + hdr->body_len) break;

        PacketHeader head = *hdr;
        std::string body = ctx_->recv_buf.substr(sizeof(PacketHeader), head.body_len);
        ctx_->recv_buf.erase(0, sizeof(PacketHeader) + head.body_len);

        auto c = ctx_;
        g_workers->enqueue([c, head, body=std::move(body)]{
            process_business(c, head, body);
        });
    }
}

void TcpConnection::handle_write() {
    while(!ctx_->send_buf.empty()){
        ssize_t n = write(fd_, ctx_->send_buf.data(), ctx_->send_buf.size());
        if(n > 0) ctx_->send_buf.erase(0, n);
        else if(n < 0 && errno == EAGAIN) return;
        else { owner_->force_close_in_loop(fd_); return; }
    }
    ctx_->writing = false;
    owner_->loop()->update_event(fd_, EPOLLIN | EPOLLRDHUP | EPOLLET, this);
}

void TcpConnection::send(const std::string& data) {
    if(closed_) return;
    if(ctx_->send_buf.size() + data.size() > Config::MAX_SEND_BUF()){
        owner_->queue_close_connection(fd_);
        return;
    }
    ctx_->send_buf.append(data);
    if(!ctx_->writing){
        ctx_->writing = true;
        owner_->loop()->update_event(fd_, EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLOUT, this);
    }
}

void TcpConnection::do_force_close() {
    if(!closed_){ closed_=true; close_impl(); }
}

void TcpConnection::close_impl() {
    owner_->loop()->remove_event(fd_);
    if(ctx_->uid > 0) {
        g_streaming_ai->cancel_request(ctx_->uid);
        SessionManager::instance().unbind(ctx_->uid);
    }
    owner_->cancel_heartbeat_timer(fd_);
    ::close(fd_);
    fd_ = -1;
    ctx_->fd = -1;
    owner_->pool()->release(ctx_);
    g_conn_count--;
}
