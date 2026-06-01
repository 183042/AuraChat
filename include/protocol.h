#pragma once
#include <cstdint>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>

#pragma pack(push, 1)
struct PacketHeader {
    uint32_t magic;
    uint32_t body_len;
    uint32_t cmd;
    uint32_t seq;
};
#pragma pack(pop)

enum class CmdType : uint32_t {
    LOGIN = 1, CHAT = 2, ACK = 3, HEARTBEAT = 4, REGISTER = 5,
    ADD_FRIEND = 6, ACCEPT_FRIEND = 7, FRIEND_LIST = 8,
    SEARCH_USER = 9, FRIEND_REQUESTS = 10
};

inline void set_keepalive(int fd, int idle = 60, int interval = 10, int count = 3) {
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
}
