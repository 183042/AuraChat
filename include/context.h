#include <chrono>
#pragma once
#include <atomic>
#include <string>
#include <vector>
#include <memory>

class Worker;

struct ClientContext {
    std::atomic<int>      fd{-1};
    std::atomic<int>      uid{0};
    std::atomic<bool>     logged_in{false};
    std::atomic<int64_t>  last_active{0};
    std::string           recv_buf;
    std::string           send_buf;
    bool                  writing{false};
    Worker*               owner_worker{nullptr};
    int                   pool_idx{-1};
    int64_t               rate_window_start{0};
    int                   rate_msg_count{0};

    void reset() {
        fd = -1; uid = 0; logged_in = false; last_active = 0;
        recv_buf.clear(); send_buf.clear(); writing = false;
        rate_window_start = 0; rate_msg_count = 0;
    }
    void update_time() {
        last_active = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

class ContextPool {
public:
    ContextPool(size_t size, Worker* owner);
    std::shared_ptr<ClientContext> acquire();
    void release(std::shared_ptr<ClientContext> ctx);
private:
    Worker* owner_;
    std::vector<std::shared_ptr<ClientContext>> storage_;
    std::vector<int> free_list_;
};
