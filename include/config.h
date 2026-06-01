#pragma once
#include <cstdlib>
#include <cstring>
#include <string>
#include <charconv>
#include <system_error>
#include "spdlog/spdlog.h"

namespace Config {
    inline int getenv_i32(const char* name, int def) {
        const char* s = std::getenv(name);
        if (!s || *s == '\0') return def;
        int val = 0;
        auto [ptr, ec] = std::from_chars(s, s + std::strlen(s), val, 10);
        if (ec == std::errc{} && ptr == s + std::strlen(s)) return val;
        if (ec == std::errc::result_out_of_range) {
            spdlog::warn("Env {} value out of int range, using default {}", name, def);
            return def;
        }
        spdlog::warn("Invalid value for env {}: '{}', using default {}", name, s, def);
        return def;
    }

    inline uint32_t getenv_u32(const char* name, uint32_t def) {
        const char* s = std::getenv(name);
        if (!s || *s == '\0') return def;
        uint32_t val = 0;
        auto [ptr, ec] = std::from_chars(s, s + std::strlen(s), val, 10);
        if (ec == std::errc{} && ptr == s + std::strlen(s)) return val;
        spdlog::warn("Invalid value for env {}: '{}', using default {}", name, s, def);
        return def;
    }

    inline constexpr uint32_t PROTO_MAGIC  = 0xDEADBEEF;
    inline constexpr uint32_t MAX_BODY_LEN = 65535;

    inline int PORT()            { return getenv_i32("IM_PORT", 8888); }
    inline int HEARTBEAT_SEC()   { return getenv_i32("IM_HB_SEC", 60); }
    inline size_t POOL_SIZE()    { return getenv_u32("IM_POOL_SIZE", 10000); }
    inline size_t MAX_SEND_BUF() { return getenv_u32("IM_MAX_SEND_BUF", 5 * 1024 * 1024); }
    inline int BACKLOG()         { return getenv_i32("IM_BACKLOG", 4096); }
    inline int MAX_CONN()        { return getenv_i32("IM_MAX_CONN", 500000); }
    inline int WORKER_NUM() {
        int v = getenv_i32("IM_WORKER", 0);
        if (v == 0) v = std::thread::hardware_concurrency();
        return v;
    }
    inline int AI_MAX_CONCURRENT() { return getenv_i32("AI_MAX_CONCURRENT", 20); }
    inline const char* DB_HOST() {
        static std::string v = []() {
            const char* e = std::getenv("DB_HOST");
            return e ? std::string(e) : "127.0.0.1";
        }();
        return v.c_str();
    }
    inline int DB_PORT()           { return getenv_i32("DB_PORT", 3306); }
    inline const char* DB_USER() {
        static std::string v = []() {
            const char* e = std::getenv("DB_USER");
            return e ? std::string(e) : "root";
        }();
        return v.c_str();
    }
    inline const char* DB_PASS() {
        static std::string v = []() {
            const char* e = std::getenv("DB_PASS");
            return e ? std::string(e) : "@Liuyingjie040907";
        }();
        return v.c_str();
    }
    inline const char* DB_NAME() {
        static std::string v = []() {
            const char* e = std::getenv("DB_NAME");
            return e ? std::string(e) : "im_chat";
        }();
        return v.c_str();
    }
    inline int RATE_LIMIT_MSGS()   { return getenv_i32("IM_RATE_LIMIT_MSGS", 20); }
    inline int RATE_LIMIT_WINDOW() { return getenv_i32("IM_RATE_LIMIT_WINDOW", 1); }
    inline const char* AI_BASE_URL() {
        static std::string url = []() {
            const char* env = std::getenv("AI_BASE_URL");
            return env ? std::string(env) : "http://127.0.0.1:8082";
        }();
        return url.c_str();
    }
}
