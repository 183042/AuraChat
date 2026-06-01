#pragma once
#include "context.h"
#include "worker.h"
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <memory>
#include "spdlog/spdlog.h"

class SessionManager {
public:
    static SessionManager& instance() { static SessionManager inst; return inst; }

    void bind(int uid, std::shared_ptr<ClientContext> ctx) {
        auto& b = buckets_[static_cast<uint32_t>(uid) % 1024];
        std::unique_lock<std::shared_mutex> l(b.mtx);
        auto it = b.m.find(uid);
        if (it != b.m.end()) {
            auto old = it->second;
            l.unlock();
            if (old && old->fd != -1) old->owner_worker->queue_close_connection(old->fd);
            l.lock();
        }
        ctx->uid = uid;
        ctx->logged_in = true;
        b.m[uid] = ctx;
        spdlog::info("Session bind: uid={}, fd={}", uid, ctx->fd.load());
    }

    void unbind(int uid) {
        auto& b = buckets_[static_cast<uint32_t>(uid) % 1024];
        std::unique_lock<std::shared_mutex> l(b.mtx);
        b.m.erase(uid);
        spdlog::info("Session unbind: uid={}", uid);
    }

    std::shared_ptr<ClientContext> get(int uid) {
        auto& b = buckets_[static_cast<uint32_t>(uid) % 1024];
        std::shared_lock<std::shared_mutex> l(b.mtx);
        auto it = b.m.find(uid);
        return it != b.m.end() ? it->second : nullptr;
    }

private:
    struct Bucket {
        std::unordered_map<int, std::shared_ptr<ClientContext>> m;
        std::shared_mutex mtx;
    };
    std::vector<Bucket> buckets_{1024};
};
