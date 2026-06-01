#pragma once
#include <cstdint>
#include <functional>
#include <chrono>
#include <set>

struct TimerNodeBase {
    int64_t expire;
    int64_t id;
    bool operator<(const TimerNodeBase& rhs) const {
        if (expire != rhs.expire) return expire < rhs.expire;
        return id < rhs.id;
    }
};

struct TimerNode : public TimerNodeBase {
    std::function<void(const TimerNode&)> func;
};

class Timer {
public:
    Timer() : gid(0) {}
    static int64_t GetTick() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    TimerNodeBase AddTimer(int64_t msec, std::function<void(const TimerNode&)> func) {
        TimerNode tnode;
        tnode.expire = GetTick() + msec;
        tnode.id = ++gid;
        tnode.func = std::move(func);
        timermap_.insert(tnode);
        return {tnode.expire, tnode.id};
    }
    bool DelTimer(const TimerNodeBase& node) {
        TimerNode temp; temp.expire = node.expire; temp.id = node.id;
        auto it = timermap_.find(temp);
        if (it != timermap_.end()) { timermap_.erase(it); return true; }
        return false;
    }
    bool CheckTimer() {
        auto it = timermap_.begin();
        if (it != timermap_.end() && it->expire <= GetTick()) {
            TimerNode copy = *it;
            timermap_.erase(it);
            if (copy.func) copy.func(copy);
            return true;
        }
        return false;
    }
    int TimeToSleep() const {
        if (timermap_.empty()) return -1;
        int64_t diss = timermap_.begin()->expire - GetTick();
        return diss > 0 ? static_cast<int>(diss) : 0;
    }
private:
    int64_t gid;
    std::set<TimerNode> timermap_;
};
