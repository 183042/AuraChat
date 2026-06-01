#pragma once
#include <string>
#include <functional>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

using json = nlohmann::json;

class LlamaClient {
public:
    LlamaClient(const std::string& url = "http://127.0.0.1:8082")
        : base_url_(url) {
        curl_ = curl_easy_init();
        if (!curl_) {
            spdlog::error("Failed to initialize CURL");
        }
    }

    ~LlamaClient() {
        if (curl_) curl_easy_cleanup(curl_);
    }

    bool stream_chat(const std::string& prompt,
                std::function<void(const std::string& chunk)> on_chunk,
                std::function<void()> on_complete = nullptr) const {
        if (!curl_) return false;

        StreamContext ctx;
        ctx.on_chunk = std::move(on_chunk);

        int max_tokens = 4096;
        bool need_long_output = false;

        if (prompt.find("代码") != std::string::npos ||
            prompt.find("写一个") != std::string::npos ||
            prompt.find("实现") != std::string::npos ||
            prompt.find("编程") != std::string::npos) {
            max_tokens = 8196;
            need_long_output = true;
        }

        if (prompt.find("详细") != std::string::npos ||
            prompt.find("全面") != std::string::npos ||
            prompt.find("完整") != std::string::npos) {
            max_tokens = std::max(max_tokens, 3072);
        }

        if (prompt.size() > 300) {
            max_tokens = std::max(max_tokens, 3072);
            need_long_output = true;
        }

        std::string system_prompt;
        if (need_long_output) {
            system_prompt = "你是一个专业助手。请在回答时保持清晰的排版，段落之间务必使用两个换行符(\\n\\n)分隔。";
        } else {
            system_prompt = "你是一个助手。请简明扼要，并使用换行符保持良好的段落排版。";
        }

        std::string json = R"({
    "messages": [
        {"role": "system", "content": ")" + escape(system_prompt) + R"("},
        {"role": "user", "content": ")" + escape(prompt) + R"("}
    ],
    "temperature": 0.7,
    "max_tokens": )" + std::to_string(max_tokens) + R"(,
    "stream": true
})";

        curl_easy_setopt(curl_, CURLOPT_URL, (base_url_ + "/v1/chat/completions").c_str());
        curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json.c_str());
        curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, stream_write_cb);
        curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &ctx);
        curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 600L);
        curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT, 60L);
        curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, 60L);
        curl_easy_setopt(curl_, CURLOPT_TCP_KEEPINTVL, 15L);
        curl_easy_setopt(curl_, CURLOPT_LOW_SPEED_LIMIT, 1L);
        curl_easy_setopt(curl_, CURLOPT_LOW_SPEED_TIME, 30L);

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, "Accept: text/event-stream");
        curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl_);
        curl_slist_free_all(headers);

        if (res != CURLE_OK) {
            spdlog::error("Stream AI request failed: {}", curl_easy_strerror(res));
            return false;
        }

        if (on_complete) on_complete();
        return true;
    }

private:
    struct StreamContext {
        std::function<void(const std::string&)> on_chunk;
        std::string line_buffer;
    };

    CURL* curl_;
    std::string base_url_;

    static size_t stream_write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
        auto* ctx = static_cast<StreamContext*>(userdata);
        size_t total = size * nmemb;
        std::string data(static_cast<char*>(ptr), total);
        ctx->line_buffer += data;

        size_t pos;
        while ((pos = ctx->line_buffer.find('\n')) != std::string::npos) {
            std::string line = ctx->line_buffer.substr(0, pos);
            ctx->line_buffer.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();

            if (line.find("data: ") == 0) {
                std::string json_data = line.substr(6);
                if (json_data == "[DONE]") continue;
                parse_stream_chunk(json_data, ctx->on_chunk);
            }
        }
        return total;
    }

    static void parse_stream_chunk(const std::string& json_raw,
                              const std::function<void(const std::string&)>& on_chunk) {
        try {
            auto j = json::parse(json_raw);
            if (j.contains("choices") && j["choices"].is_array() && !j["choices"].empty()) {
                auto& delta = j["choices"][0]["delta"];
                std::string content;
                if (delta.contains("content") && !delta["content"].is_null()) {
                    content = delta["content"].get<std::string>();
                }
                else if (delta.contains("reasoning_content") && !delta["reasoning_content"].is_null()) {
                    content = delta["reasoning_content"].get<std::string>();
                }
                if (!content.empty() && on_chunk) {
                    on_chunk(content);
                }
            }
        } catch (const std::exception& e) {
            // Ignore parse errors on keepalive / DONE
        }
    }

    static std::string escape(const std::string& s) {
        std::string out;
        for (char c : s) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default: out += c;
            }
        }
        return out;
    }
};
