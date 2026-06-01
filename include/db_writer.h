#pragma once
#include <string>
#include <vector>
#include <tuple>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <chrono>
#include <mysql/mysql.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct DBChatMessage {
    int64_t id = 0;
    int from_uid = 0, to_uid = 0;
    std::string content;
    std::chrono::system_clock::time_point ts;
    bool delivered = true;
};

class DBBatchWriter {
public:
    DBBatchWriter();
    ~DBBatchWriter();
    void submit(DBChatMessage msg);

    int register_user(const std::string& username, const std::string& password);
    int verify_login(const std::string& username, const std::string& password);
    std::vector<DBChatMessage> query_offline_messages(int uid);
    std::vector<std::pair<int, std::string>> search_users(const std::string& keyword);
    int add_friend_request(int from_uid, int to_uid, const std::string& msg);
    int accept_friend_request(int request_id, int acceptor_uid);
    std::vector<std::pair<int, std::string>> get_friends(int uid);
    std::vector<std::tuple<int, int, std::string, std::string>> get_pending_requests(int uid);
    std::string get_username(int uid);
    void mark_delivered(int uid, int64_t max_id);

private:
    std::string esc(const std::string& s);
    bool connect_db();
    bool ensure_connected();

    std::string file_data_dir_ = "data";
    bool use_file_storage_ = false;

    struct FileUser {
        int uid;
        std::string username;
        std::string password_hash;
    };
    struct FileFriendRequest {
        int id;
        int from_uid;
        int to_uid;
        std::string message;
        int status;
    };
    struct FileMessage {
        int id;
        int from_uid;
        int to_uid;
        std::string content;
        bool delivered;
    };

    std::vector<FileUser> file_users_;
    std::vector<std::pair<int,int>> file_friends_;
    std::vector<FileFriendRequest> file_friend_reqs_;
    std::vector<FileMessage> file_messages_;
    int file_next_uid_ = 1000;
    int file_next_req_id_ = 1;
    int file_next_msg_id_ = 1;
    std::mutex file_mutex_;

    void load_file_data();
    void save_users_file();
    void save_friends_file();
    void save_friend_reqs_file();
    void save_messages_file();
    std::string file_hash(const std::string& input);
    void loop();

    std::queue<DBChatMessage> queue_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::atomic<bool> running_;
    std::thread thread_;
    MYSQL* mysql_{nullptr};
};
