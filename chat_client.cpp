#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include <condition_variable>
#include <sstream>
#include <algorithm>
#include "message.pb.h"

using namespace my_chat;

constexpr uint32_t PROTO_MAGIC = 0xDEADBEEF;

#pragma pack(push, 1)
struct PacketHeader {
    uint32_t magic;
    uint32_t body_len;
    uint32_t cmd;
    uint32_t seq;
};
#pragma pack(pop)

enum class CmdType : uint32_t { LOGIN = 1, CHAT = 2, ACK = 3, HEARTBEAT = 4, REGISTER = 5 };

class AIChatClient {
public:
    AIChatClient(const std::string& host, uint16_t port)
        : host_(host), port_(port), uid_(-1), sock_(-1), running_(false),
          in_stream_(false), chat_target_(9999) {}

    ~AIChatClient() { disconnect(); }

    bool connect() {
        sock_ = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_ < 0) {
            std::cerr << "创建 socket 失败" << std::endl;
            return false;
        }
        struct timeval tv{1, 0};
        setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);
        if (::connect(sock_, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "连接服务器失败: " << strerror(errno) << std::endl;
            close(sock_);
            return false;
        }
        running_ = true;
        recv_thread_ = std::thread(&AIChatClient::receive_loop, this);
        std::ios::sync_with_stdio(false);
        std::cout.setf(std::ios::unitbuf);
        print_welcome();
        return true;
    }

    void disconnect() {
        running_ = false;
        cv_.notify_all();
        if (sock_ >= 0) { shutdown(sock_, SHUT_RDWR); close(sock_); sock_ = -1; }
        if (recv_thread_.joinable()) recv_thread_.join();
        if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    }

    bool is_logged_in() const { return uid_ > 0; }

    // ===== 注册 =====
    bool do_register(const std::string& username, const std::string& password) {
        if (username.empty() || password.empty()) {
            std::cout << "❌ 用户名和密码不能为空" << std::endl;
            return false;
        }
        RegisterRequest req;
        req.set_username(username);
        req.set_password(password);
        std::string body;
        req.SerializeToString(&body);
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_response_ = PendingAuth{true, -1, ""};
        }
        if (!send_packet((uint32_t)CmdType::REGISTER, body)) return false;
        return wait_auth_result("注册");
    }

    // ===== 登录 =====
    bool do_login(const std::string& username, const std::string& password) {
        if (username.empty() || password.empty()) {
            std::cout << "❌ 用户名和密码不能为空" << std::endl;
            return false;
        }
        LoginRequest req;
        req.set_username(username);
        req.set_password(password);
        std::string body;
        req.SerializeToString(&body);
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_response_ = PendingAuth{true, -1, ""};
        }
        if (!send_packet((uint32_t)CmdType::LOGIN, body)) return false;
        return wait_auth_result("登录");
    }

    bool send_message(int to_uid, const std::string& text) {
        if (!is_logged_in()) { std::cout << "❌ 请先登录" << std::endl; return false; }
        ChatMessage msg;
        msg.set_from_uid(uid_);
        msg.set_to_uid(to_uid);
        msg.set_content(text);
        msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        std::string body;
        if (!msg.SerializeToString(&body)) { std::cerr << "序列化消息失败" << std::endl; return false; }
        return send_packet((uint32_t)CmdType::CHAT, body);
    }

    void chat_loop() {
        std::string line;
        while (running_) {
            if (!in_stream_ && is_logged_in()) {
                if (chat_target_ == 9999)
                    std::cout << "\r[🤖 AI] >>> " << std::flush;
                else
                    std::cout << "\r[👤 用户" << chat_target_ << "] >>> " << std::flush;
            } else if (!is_logged_in()) {
                std::cout << "\r[未登录] >>> " << std::flush;
            }
            if (!std::getline(std::cin, line)) break;
            if (line.empty()) continue;
            if (handle_command(line)) continue;
            if (!is_logged_in()) {
                std::cout << "❌ 请先注册或登录: /register <用户名> <密码>  或  /login <用户名> <密码>" << std::endl;
                continue;
            }
            if (chat_target_ == 9999) {
                { std::lock_guard<std::mutex> lock(wait_mutex_); stream_completed_ = false; stream_started_ = false; }
                last_ai_chunk_time_ = std::chrono::steady_clock::now();
                if (!send_message(9999, line)) { std::cerr << "\n发送失败" << std::endl; break; }
                wait_for_stream_completion();
            } else {
                std::cout << "📤 发送给用户" << chat_target_ << ": " << line << std::endl;
                if (!send_message(chat_target_, line)) std::cerr << "\n发送失败" << std::endl;
            }
        }
    }

private:
    struct PendingAuth { bool active; int uid; std::string msg; };

    std::string host_;
    uint16_t port_;
    int uid_;
    int sock_;
    std::atomic<bool> running_;
    std::thread recv_thread_;
    std::thread heartbeat_thread_;
    uint32_t seq_ = 0;
    std::atomic<bool> in_stream_;
    std::chrono::steady_clock::time_point last_ai_chunk_time_;
    std::mutex wait_mutex_;
    std::condition_variable cv_;
    bool stream_completed_ = false;
    bool stream_started_ = false;
    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    PendingAuth pending_response_{false, -1, ""};
    int chat_target_{9999};
    std::vector<int> recent_contacts_;

    bool wait_auth_result(const std::string& action) {
        std::unique_lock<std::mutex> lock(pending_mutex_);
        if (!pending_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [this] { return !pending_response_.active; })) {
            std::cout << "⏰ " << action << "超时" << std::endl;
            pending_response_.active = false;
            return false;
        }
        if (pending_response_.uid > 0) {
            uid_ = pending_response_.uid;
            std::cout << "✅ " << action << "成功! UID: " << uid_ << std::endl;
            start_heartbeat();
            return true;
        }
        std::cout << "❌ " << action << "失败: " << pending_response_.msg << std::endl;
        return false;
    }

    void print_welcome() {
        std::cout << "========================================" << std::endl;
        std::cout << "🔌 已连接到 IM 服务器" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "📝 请先注册或登录:" << std::endl;
        std::cout << "  /register <用户名> <密码>  — 注册新账号" << std::endl;
        std::cout << "  /login <用户名> <密码>     — 登录已有账号" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "💡 登录后可用命令:" << std::endl;
        std::cout << "  /ai              — 切换到 AI 对话" << std::endl;
        std::cout << "  /chat <uid>      — 切换到私聊" << std::endl;
        std::cout << "  /to <uid> <消息>  — 快速发送给指定用户" << std::endl;
        std::cout << "  /list            — 最近联系人" << std::endl;
        std::cout << "  /status          — 当前状态" << std::endl;
        std::cout << "  /quit            — 退出" << std::endl;
        std::cout << "========================================" << std::endl;
    }

    bool handle_command(const std::string& line) {
        if (line.empty() || line[0] != '/') return false;
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd == "/register") {
            std::string u, p;
            if (iss >> u >> p) return do_register(u, p);
            std::cout << "❌ 用法: /register <用户名> <密码>" << std::endl;
            return true;
        }
        if (cmd == "/login") {
            std::string u, p;
            if (iss >> u >> p) return do_login(u, p);
            std::cout << "❌ 用法: /login <用户名> <密码>" << std::endl;
            return true;
        }
        if (!is_logged_in()) { std::cout << "❌ 请先注册或登录" << std::endl; return true; }
        if (cmd == "/ai") { chat_target_ = 9999; std::cout << "✅ 已切换到 AI 对话模式" << std::endl; return true; }
        if (cmd == "/chat") {
            int t; if (iss >> t) {
                if (t == uid_) { std::cout << "❌ 不能和自己私聊" << std::endl; return true; }
                chat_target_ = t; add_recent_contact(t);
                std::cout << "✅ 已切换到与用户" << t << "的私聊模式" << std::endl;
            } else std::cout << "❌ 用法: /chat <uid>" << std::endl;
            return true;
        }
        if (cmd == "/to") {
            int t; std::string m;
            if (iss >> t) {
                std::getline(iss, m);
                if (!m.empty() && m[0] == ' ') m = m.substr(1);
                if (!m.empty()) { add_recent_contact(t); std::cout << "📤 发送给用户" << t << ": " << m << std::endl; return send_message(t, m); }
            }
            std::cout << "❌ 用法: /to <uid> <消息>" << std::endl;
            return true;
        }
        if (cmd == "/list") {
            if (recent_contacts_.empty()) std::cout << "📋 暂无最近联系人" << std::endl;
            else {
                std::cout << "📋 最近联系人:" << std::endl;
                for (auto c : recent_contacts_)
                    std::cout << "  👤 用户" << c << (c == chat_target_ ? " ← 当前" : "") << std::endl;
            }
            return true;
        }
        if (cmd == "/status") {
            std::cout << "📊 当前状态:" << std::endl
                      << "  👤 我的 UID: " << uid_ << std::endl
                      << "  🎯 当前目标: " << (chat_target_ == 9999 ? "AI助手" : "用户" + std::to_string(chat_target_)) << std::endl
                      << "  📡 连接: " << (running_ ? "在线" : "离线") << std::endl;
            return true;
        }
        if (cmd == "/help") { print_welcome(); return true; }
        if (cmd == "/quit") { running_ = false; return true; }
        std::cout << "❌ 未知命令: " << cmd << " (输入 /help 查看帮助)" << std::endl;
        return true;
    }

    void add_recent_contact(int uid) {
        recent_contacts_.erase(std::remove(recent_contacts_.begin(), recent_contacts_.end(), uid), recent_contacts_.end());
        recent_contacts_.insert(recent_contacts_.begin(), uid);
        if (recent_contacts_.size() > 10) recent_contacts_.pop_back();
    }

    bool send_packet(uint32_t cmd, const std::string& body) {
        uint32_t seq = ++seq_;
        PacketHeader hdr{PROTO_MAGIC, (uint32_t)body.size(), cmd, seq};
        if (::send(sock_, &hdr, sizeof(hdr), MSG_NOSIGNAL) < 0) return false;
        if (!body.empty() && ::send(sock_, body.data(), body.size(), MSG_NOSIGNAL) < 0) return false;
        return true;
    }

    void start_heartbeat() {
        if (heartbeat_thread_.joinable()) return;
        heartbeat_thread_ = std::thread([this] {
            while (running_) {
                std::this_thread::sleep_for(std::chrono::seconds(30));
                if (running_ && uid_ > 0) {
                    ChatMessage hb;
                    hb.set_from_uid(uid_);
                    hb.set_to_uid(0);
                    hb.set_content("PING");
                    std::string body;
                    hb.SerializeToString(&body);
                    send_packet((uint32_t)CmdType::HEARTBEAT, body);
                }
            }
        });
    }

    void wait_for_stream_completion() {
        std::unique_lock<std::mutex> lock(wait_mutex_);
        if (!cv_.wait_for(lock, std::chrono::seconds(180), [this] { return !running_ || stream_started_; })) {
            std::cout << "\n⏰ AI 响应超时" << std::endl; return;
        }
        while (running_ && !stream_completed_) {
            cv_.wait_for(lock, std::chrono::seconds(30));
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - last_ai_chunk_time_).count();
            if (elapsed > 30) { std::cout << "\n⏰ AI 响应中断" << std::endl; break; }
        }
        in_stream_ = false;
        std::cout << std::endl;
    }

    void receive_loop() {
        std::vector<char> buffer;
        char buf[4096];
        while (running_) {
            ssize_t n = recv(sock_, buf, sizeof(buf), 0);
            if (n == 0) {
                std::cout << "\n⚠️ 服务器已关闭连接" << std::endl;
                running_ = false; cv_.notify_all(); break;
            } else if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                if (running_) std::cout << "\n⚠️ 接收错误: " << strerror(errno) << std::endl;
                running_ = false; cv_.notify_all(); break;
            }
            buffer.insert(buffer.end(), buf, buf + n);
            while (buffer.size() >= sizeof(PacketHeader)) {
                PacketHeader* hdr = reinterpret_cast<PacketHeader*>(buffer.data());
                if (hdr->magic != PROTO_MAGIC) { buffer.erase(buffer.begin()); continue; }
                uint32_t full_len = sizeof(PacketHeader) + hdr->body_len;
                if (buffer.size() < full_len) break;
                std::string body(buffer.begin() + sizeof(PacketHeader), buffer.begin() + full_len);
                uint32_t cmd = hdr->cmd;
                buffer.erase(buffer.begin(), buffer.begin() + full_len);
                if (cmd == (uint32_t)CmdType::CHAT) {
                    ChatMessage msg;
                    if (msg.ParseFromString(body)) handle_chat_message(msg);
                } else if (cmd == (uint32_t)CmdType::ACK) {
                    Response resp;
                    if (resp.ParseFromString(body)) handle_ack_response(resp);
                }
            }
        }
    }

    void handle_chat_message(const ChatMessage& msg) {
        last_ai_chunk_time_ = std::chrono::steady_clock::now();
        if (msg.from_uid() != 9999) {
            if (!in_stream_) {
                std::cout << "\r\033[K📩 ";
                if (msg.from_uid() == chat_target_) std::cout << "【当前聊天】";
                std::cout << "用户" << msg.from_uid() << ": " << msg.content() << std::endl;
            }
            return;
        }
        // AI 流式
        {
            std::lock_guard<std::mutex> lock(wait_mutex_);
            if (msg.is_last_chunk()) { in_stream_ = false; stream_completed_ = true; cv_.notify_all(); return; }
            if (msg.content().empty()) return;
            if (!in_stream_) {
                in_stream_ = true; stream_started_ = true; cv_.notify_all();
                std::cout << "\r\033[K🤖 AI: " << std::flush;
            }
            std::cout << msg.content() << std::flush;
        }
    }

    void handle_ack_response(const Response& resp) {
        if (resp.msg() == "PONG") return;
        // 检查是否是注册/登录的响应
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            if (pending_response_.active) {
                pending_response_.uid = resp.uid();
                pending_response_.msg = resp.msg();
                pending_response_.active = false;
                pending_cv_.notify_all();
                return;
            }
        }
        std::cout << "\r\033[K";
        if (resp.success()) std::cout << "[系统] " << resp.msg() << std::endl;
        else std::cout << "[系统] 错误: " << resp.msg() << std::endl;
    }
};

int main(int argc, char* argv[]) {
    std::string host = (argc >= 2) ? argv[1] : "127.0.0.1";
    uint16_t port = (argc >= 3) ? (uint16_t)std::stoi(argv[2]) : 8888;
    AIChatClient client(host, port);
    if (!client.connect()) return 1;
    client.chat_loop();
    std::cout << "\n👋 再见！" << std::endl;
    return 0;
}

