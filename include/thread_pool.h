#pragma once
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

class ThreadPool {
public:
    ThreadPool(size_t n) {
        for (size_t i = 0; i < n; ++i) {
            threads_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> l(mutex_);
                        cv_.wait(l, [this] { return stop_ || !tasks_.empty(); });
                        if (stop_ && tasks_.empty()) return;
                        task = std::move(tasks_.front()); tasks_.pop();
                    }
                    task();
                }
            });
        }
    }
    ~ThreadPool() {
        { std::unique_lock<std::mutex> l(mutex_); stop_ = true; }
        cv_.notify_all();
        for (auto& t : threads_) t.join();
    }
    bool enqueue(std::function<void()> task) {
        std::unique_lock<std::mutex> l(mutex_);
        if (tasks_.size() > 200000) return false;
        tasks_.push(std::move(task));
        cv_.notify_one();
        return true;
    }
private:
    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_{false};
};
