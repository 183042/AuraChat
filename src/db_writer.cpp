#include "db_writer.h"
#include "config.h"
#include "spdlog/spdlog.h"
#include <sys/stat.h>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>

DBBatchWriter::DBBatchWriter() : running_(true), thread_([this]{ loop(); }) {
    if (!connect_db()) {
        spdlog::warn("MySQL unavailable, using file-based storage");
        use_file_storage_ = true;
        load_file_data();
    }
}

DBBatchWriter::~DBBatchWriter() {
    running_ = false;
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    if (mysql_) mysql_close(mysql_);
}

void DBBatchWriter::submit(DBChatMessage msg) {
    { std::lock_guard<std::mutex> l(mtx_); queue_.push(std::move(msg)); }
    cv_.notify_one();
}

std::string DBBatchWriter::esc(const std::string& s) {
    size_t len = s.size();
    auto buf = std::make_unique<char[]>(len * 2 + 1);
    mysql_real_escape_string(mysql_, buf.get(), s.data(), len);
    return std::string(buf.get());
}

bool DBBatchWriter::connect_db() {
    if (mysql_) mysql_close(mysql_);
    mysql_ = mysql_init(nullptr);
    if (!mysql_) return false;
    if (!mysql_real_connect(mysql_, Config::DB_HOST(), Config::DB_USER(),
                            Config::DB_PASS(), Config::DB_NAME(),
                            Config::DB_PORT(), nullptr, 0)) {
        spdlog::error("MySQL connect failed: {}", mysql_error(mysql_));
        return false;
    }
    const char* create_users =
        "CREATE TABLE IF NOT EXISTS users ("
        "  uid INT AUTO_INCREMENT PRIMARY KEY,"
        "  username VARCHAR(64) NOT NULL UNIQUE,"
        "  password_hash CHAR(64) NOT NULL,"
        "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    if (mysql_query(mysql_, create_users)) {
        spdlog::error("Create users table failed: {}", mysql_error(mysql_));
        return false;
    }
    const char* create_msgs =
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        "  from_uid INT NOT NULL,"
        "  to_uid INT NOT NULL,"
        "  content TEXT NOT NULL,"
        "  delivered TINYINT(1) NOT NULL DEFAULT 1,"
        "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "  INDEX idx_from_uid (from_uid),"
        "  INDEX idx_to_uid (to_uid),"
        "  INDEX idx_delivered (delivered),"
        "  INDEX idx_created_at (created_at)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    if (mysql_query(mysql_, create_msgs)) {
        spdlog::error("Create messages table failed: {}", mysql_error(mysql_));
        return false;
    }
    const char* create_friends =
        "CREATE TABLE IF NOT EXISTS friends ("
        "  id INT AUTO_INCREMENT PRIMARY KEY,"
        "  uid1 INT NOT NULL,"
        "  uid2 INT NOT NULL,"
        "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "  UNIQUE INDEX idx_pair ((LEAST(uid1,uid2)), (GREATEST(uid1,uid2))),"
        "  INDEX idx_uid1 (uid1),"
        "  INDEX idx_uid2 (uid2)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    if (mysql_query(mysql_, create_friends)) {
        spdlog::error("Create friends table failed: {}", mysql_error(mysql_));
        return false;
    }
    const char* create_friend_reqs =
        "CREATE TABLE IF NOT EXISTS friend_requests ("
        "  id INT AUTO_INCREMENT PRIMARY KEY,"
        "  from_uid INT NOT NULL,"
        "  to_uid INT NOT NULL,"
        "  message VARCHAR(255) DEFAULT '',"
        "  status TINYINT(1) NOT NULL DEFAULT 0,"
        "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "  INDEX idx_to_uid (to_uid),"
        "  INDEX idx_from_uid (from_uid)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    if (mysql_query(mysql_, create_friend_reqs)) {
        spdlog::error("Create friend_requests table failed: {}", mysql_error(mysql_));
        return false;
    }
    spdlog::info("DB connected: {}:{}/{}", Config::DB_HOST(), Config::DB_PORT(), Config::DB_NAME());
    return true;
}

bool DBBatchWriter::ensure_connected() {
    if (use_file_storage_) return true;
    if (mysql_ && !mysql_ping(mysql_)) return true;
    spdlog::warn("DB connection lost, reconnecting...");
    return connect_db();
}

int DBBatchWriter::register_user(const std::string& username, const std::string& password) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        for (auto& u : file_users_)
            if (u.username == username) return -2;
        int uid = file_next_uid_++;
        file_users_.push_back({uid, username, file_hash(password)});
        save_users_file();
        spdlog::info("File: User registered: {} -> uid {}", username, uid);
        return uid;
    }
    if (!ensure_connected()) return -1;
    std::string check_sql = "SELECT uid FROM users WHERE username='";
    check_sql += esc(username) + "'";
    if (mysql_query(mysql_, check_sql.c_str()) == 0) {
        MYSQL_RES* res = mysql_store_result(mysql_);
        if (res && mysql_num_rows(res) > 0) {
            mysql_free_result(res);
            return -2;
        }
        if (res) mysql_free_result(res);
    }
    std::string sql = "INSERT INTO users (username,password_hash) VALUES ('";
    sql += esc(username) + "',SHA2('" + esc(password) + "',256))";
    if (mysql_query(mysql_, sql.c_str())) {
        spdlog::error("Register failed: {}", mysql_error(mysql_));
        return -1;
    }
    int uid = (int)mysql_insert_id(mysql_);
    spdlog::info("User registered: {} -> uid {}", username, uid);
    return uid;
}

int DBBatchWriter::verify_login(const std::string& username, const std::string& password) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        std::string h = file_hash(password);
        for (auto& u : file_users_)
            if (u.username == username && u.password_hash == h) return u.uid;
        return -1;
    }
    if (!ensure_connected()) return -1;
    std::string sql = "SELECT uid FROM users WHERE username='";
    sql += esc(username) + "' AND password_hash=SHA2('" + esc(password) + "',256)";
    if (mysql_query(mysql_, sql.c_str())) {
        spdlog::error("Login query failed: {}", mysql_error(mysql_));
        return -1;
    }
    MYSQL_RES* res = mysql_store_result(mysql_);
    if (!res) return -1;
    int uid = -1;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row) uid = std::stoi(row[0]);
    mysql_free_result(res);
    return uid;
}

std::vector<DBChatMessage> DBBatchWriter::query_offline_messages(int uid) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        std::vector<DBChatMessage> msgs;
        for (auto& m : file_messages_) {
            if (m.to_uid == uid && !m.delivered) {
                DBChatMessage dm;
                dm.id = m.id;
                dm.from_uid = m.from_uid;
                dm.to_uid = m.to_uid;
                dm.content = m.content;
                dm.delivered = false;
                msgs.push_back(std::move(dm));
            }
        }
        return msgs;
    }
    std::vector<DBChatMessage> msgs;
    if (!ensure_connected()) return msgs;
    std::string sql = "SELECT id,from_uid,to_uid,content FROM messages WHERE to_uid=";
    sql += std::to_string(uid) + " AND delivered=0 ORDER BY id";
    if (mysql_query(mysql_, sql.c_str())) return msgs;
    MYSQL_RES* res = mysql_store_result(mysql_);
    if (!res) return msgs;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
        DBChatMessage m;
        m.id = std::stoll(row[0]);
        m.from_uid = std::stoi(row[1]);
        m.to_uid = std::stoi(row[2]);
        m.content = row[3] ? row[3] : "";
        m.delivered = false;
        msgs.push_back(std::move(m));
    }
    mysql_free_result(res);
    return msgs;
}

std::vector<std::pair<int, std::string>> DBBatchWriter::search_users(const std::string& keyword) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        std::vector<std::pair<int, std::string>> result;
        std::string kw_lower = keyword;
        std::transform(kw_lower.begin(), kw_lower.end(), kw_lower.begin(), ::tolower);
        for (auto& u : file_users_) {
            std::string uname_lower = u.username;
            std::transform(uname_lower.begin(), uname_lower.end(), uname_lower.begin(), ::tolower);
            if (uname_lower.find(kw_lower) != std::string::npos)
                result.push_back({u.uid, u.username});
            if (result.size() >= 20) break;
        }
        return result;
    }
    std::vector<std::pair<int, std::string>> result;
    if (!ensure_connected()) return result;
    std::string sql = "SELECT uid,username FROM users WHERE username LIKE '%";
    sql += esc(keyword) + "%' LIMIT 20";
    if (mysql_query(mysql_, sql.c_str())) return result;
    MYSQL_RES* res = mysql_store_result(mysql_);
    if (!res) return result;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)))
        result.push_back({std::stoi(row[0]), row[1] ? row[1] : ""});
    mysql_free_result(res);
    return result;
}

int DBBatchWriter::add_friend_request(int from_uid, int to_uid, const std::string& msg) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        int lo = std::min(from_uid, to_uid);
        int hi = std::max(from_uid, to_uid);
        for (auto& p : file_friends_)
            if (p.first == lo && p.second == hi) return -2;
        for (auto& r : file_friend_reqs_)
            if (r.from_uid == from_uid && r.to_uid == to_uid && r.status == 0) return -3;
        int rid = file_next_req_id_++;
        file_friend_reqs_.push_back({rid, from_uid, to_uid, msg, 0});
        save_friend_reqs_file();
        spdlog::info("File: Friend request: {} -> {}", from_uid, to_uid);
        return rid;
    }
    if (!ensure_connected()) return -1;
    std::string check = "SELECT id FROM friends WHERE (uid1=LEAST(" +
        std::to_string(from_uid) + "," + std::to_string(to_uid) + ") AND uid2=GREATEST(" +
        std::to_string(from_uid) + "," + std::to_string(to_uid) + "))";
    if (mysql_query(mysql_, check.c_str()) == 0) {
        MYSQL_RES* r = mysql_store_result(mysql_);
        if (r && mysql_num_rows(r) > 0) { mysql_free_result(r); return -2; }
        if (r) mysql_free_result(r);
    }
    std::string dup = "SELECT id FROM friend_requests WHERE from_uid=" +
        std::to_string(from_uid) + " AND to_uid=" + std::to_string(to_uid) + " AND status=0";
    if (mysql_query(mysql_, dup.c_str()) == 0) {
        MYSQL_RES* r = mysql_store_result(mysql_);
        if (r && mysql_num_rows(r) > 0) { mysql_free_result(r); return -3; }
        if (r) mysql_free_result(r);
    }
    std::string sql = "INSERT INTO friend_requests (from_uid,to_uid,message) VALUES (" +
        std::to_string(from_uid) + "," + std::to_string(to_uid) + ",'" + esc(msg) + "')";
    if (mysql_query(mysql_, sql.c_str())) { spdlog::error("Add friend request failed: {}", mysql_error(mysql_)); return -1; }
    spdlog::info("Friend request: {} -> {}", from_uid, to_uid);
    return (int)mysql_insert_id(mysql_);
}

int DBBatchWriter::accept_friend_request(int request_id, int acceptor_uid) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        int from_uid = -1;
        for (auto& r : file_friend_reqs_) {
            if (r.id == request_id && r.to_uid == acceptor_uid && r.status == 0) {
                from_uid = r.from_uid;
                r.status = 1;
                break;
            }
        }
        if (from_uid < 0) return -1;
        int lo = std::min(from_uid, acceptor_uid);
        int hi = std::max(from_uid, acceptor_uid);
        file_friends_.push_back({lo, hi});
        save_friend_reqs_file();
        save_friends_file();
        spdlog::info("File: Friend accepted: {} <-> {}", from_uid, acceptor_uid);
        return from_uid;
    }
    if (!ensure_connected()) return -1;
    std::string check = "SELECT from_uid,to_uid FROM friend_requests WHERE id=" +
        std::to_string(request_id) + " AND to_uid=" + std::to_string(acceptor_uid) + " AND status=0";
    if (mysql_query(mysql_, check.c_str())) return -1;
    MYSQL_RES* r = mysql_store_result(mysql_);
    if (!r || mysql_num_rows(r) == 0) { if(r) mysql_free_result(r); return -1; }
    MYSQL_ROW row = mysql_fetch_row(r);
    int from_uid = std::stoi(row[0]);
    int to_uid = std::stoi(row[1]);
    mysql_free_result(r);
    std::string upd = "UPDATE friend_requests SET status=1 WHERE id=" + std::to_string(request_id);
    mysql_query(mysql_, upd.c_str());
    std::string ins = "INSERT INTO friends (uid1,uid2) VALUES (" +
        std::to_string(std::min(from_uid, to_uid)) + "," + std::to_string(std::max(from_uid, to_uid)) + ")";
    if (mysql_query(mysql_, ins.c_str())) { spdlog::error("Insert friend failed: {}", mysql_error(mysql_)); return -1; }
    spdlog::info("Friend accepted: {} <-> {}", from_uid, to_uid);
    return from_uid;
}

std::vector<std::pair<int, std::string>> DBBatchWriter::get_friends(int uid) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        std::vector<std::pair<int, std::string>> result;
        for (auto& p : file_friends_) {
            int other = 0;
            if (p.first == uid) other = p.second;
            else if (p.second == uid) other = p.first;
            else continue;
            std::string name = "用户" + std::to_string(other);
            for (auto& u : file_users_)
                if (u.uid == other) { name = u.username; break; }
            result.push_back({other, name});
        }
        return result;
    }
    std::vector<std::pair<int, std::string>> result;
    if (!ensure_connected()) return result;
    std::string sql = "SELECT u.uid,u.username FROM users u INNER JOIN friends f ON "
        "(u.uid=f.uid1 OR u.uid=f.uid2) WHERE (f.uid1=" + std::to_string(uid) +
        " OR f.uid2=" + std::to_string(uid) + ") AND u.uid!=" + std::to_string(uid);
    if (mysql_query(mysql_, sql.c_str())) return result;
    MYSQL_RES* res = mysql_store_result(mysql_);
    if (!res) return result;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)))
        result.push_back({std::stoi(row[0]), row[1] ? row[1] : ""});
    mysql_free_result(res);
    return result;
}

std::vector<std::tuple<int, int, std::string, std::string>> DBBatchWriter::get_pending_requests(int uid) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        std::vector<std::tuple<int, int, std::string, std::string>> result;
        for (auto it = file_friend_reqs_.rbegin(); it != file_friend_reqs_.rend(); ++it) {
            if (it->to_uid == uid && it->status == 0) {
                std::string name = "用户" + std::to_string(it->from_uid);
                for (auto& u : file_users_)
                    if (u.uid == it->from_uid) { name = u.username; break; }
                result.push_back({it->id, it->from_uid, name, it->message});
            }
        }
        return result;
    }
    std::vector<std::tuple<int, int, std::string, std::string>> result;
    if (!ensure_connected()) return result;
    std::string sql = "SELECT fr.id,fr.from_uid,u.username,fr.message FROM friend_requests fr "
        "INNER JOIN users u ON fr.from_uid=u.uid WHERE fr.to_uid=" +
        std::to_string(uid) + " AND fr.status=0 ORDER BY fr.id DESC";
    if (mysql_query(mysql_, sql.c_str())) return result;
    MYSQL_RES* res = mysql_store_result(mysql_);
    if (!res) return result;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)))
        result.push_back({std::stoi(row[0]), std::stoi(row[1]), row[2] ? row[2] : "", row[3] ? row[3] : ""});
    mysql_free_result(res);
    return result;
}

std::string DBBatchWriter::get_username(int uid) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        for (auto& u : file_users_)
            if (u.uid == uid) return u.username;
        return "";
    }
    if (!ensure_connected()) return "";
    std::string sql = "SELECT username FROM users WHERE uid=" + std::to_string(uid);
    if (mysql_query(mysql_, sql.c_str())) return "";
    MYSQL_RES* r = mysql_store_result(mysql_);
    if (!r) return "";
    std::string name;
    MYSQL_ROW row = mysql_fetch_row(r);
    if (row) name = row[0] ? row[0] : "";
    mysql_free_result(r);
    return name;
}
void DBBatchWriter::mark_delivered(int uid, int64_t max_id) {
    if (use_file_storage_) {
        std::lock_guard<std::mutex> l(file_mutex_);
        for (auto& m : file_messages_)
            if (m.to_uid == uid && m.id <= max_id) m.delivered = true;
        save_messages_file();
        return;
    }
    if (!ensure_connected()) return;
    std::string sql = "UPDATE messages SET delivered=1 WHERE to_uid=";
    sql += std::to_string(uid) + " AND delivered=0 AND id<=" + std::to_string(max_id);
    if (mysql_query(mysql_, sql.c_str()))
        spdlog::error("Mark delivered failed: {}", mysql_error(mysql_));
}
void DBBatchWriter::load_file_data() {
    std::lock_guard<std::mutex> l(file_mutex_);
    mkdir(file_data_dir_.c_str(), 0755);
    auto load_json = [&](const char* fn) -> json {
        std::string path = file_data_dir_ + "/" + fn;
        std::ifstream f(path);
        if (!f.is_open()) return json::object();
        try { return json::parse(f); }
        catch (...) { return json::object(); }
    };

    auto j_users = load_json("users.json");
    file_next_uid_ = j_users.value("next_uid", 1000);
    file_users_.clear();
    if (j_users.contains("users")) {
        for (auto& u : j_users["users"]) {
            file_users_.push_back({u["uid"], u["username"], u["password_hash"]});
        }
    }

    auto j_friends = load_json("friends.json");
    file_friends_.clear();
    if (j_friends.contains("pairs")) {
        for (auto& p : j_friends["pairs"]) {
            file_friends_.push_back({p[0], p[1]});
        }
    }

    auto j_reqs = load_json("friend_requests.json");
    file_next_req_id_ = j_reqs.value("next_id", 1);
    file_friend_reqs_.clear();
    if (j_reqs.contains("requests")) {
        for (auto& r : j_reqs["requests"]) {
            file_friend_reqs_.push_back({r["id"], r["from_uid"], r["to_uid"],
                r.value("message", ""), r.value("status", 0)});
        }
    }

    auto j_msgs = load_json("messages.json");
    file_next_msg_id_ = j_msgs.value("next_id", 1);
    file_messages_.clear();
    if (j_msgs.contains("messages")) {
        for (auto& m : j_msgs["messages"]) {
            file_messages_.push_back({m["id"], m["from_uid"], m["to_uid"],
                m["content"], m.value("delivered", true)});
        }
    }
    spdlog::info("File storage loaded: {} users, {} friends",
        file_users_.size(), file_friends_.size());
}

void DBBatchWriter::save_users_file() {
    json j;
    j["next_uid"] = file_next_uid_;
    json users_arr = json::array();
    for (auto& u : file_users_) {
        users_arr.push_back({{"uid", u.uid}, {"username", u.username},
            {"password_hash", u.password_hash}});
    }
    j["users"] = users_arr;
    std::string path = file_data_dir_ + "/users.json";
    std::ofstream f(path);
    if (f.is_open()) f << j.dump(2);
}

void DBBatchWriter::save_friends_file() {
    json j;
    json pairs = json::array();
    for (auto& p : file_friends_)
        pairs.push_back({p.first, p.second});
    j["pairs"] = pairs;
    std::ofstream f(file_data_dir_ + "/friends.json");
    if (f.is_open()) f << j.dump(2);
}

void DBBatchWriter::save_friend_reqs_file() {
    json j;
    j["next_id"] = file_next_req_id_;
    json arr = json::array();
    for (auto& r : file_friend_reqs_) {
        arr.push_back({{"id", r.id}, {"from_uid", r.from_uid},
            {"to_uid", r.to_uid}, {"message", r.message}, {"status", r.status}});
    }
    j["requests"] = arr;
    std::ofstream f(file_data_dir_ + "/friend_requests.json");
    if (f.is_open()) f << j.dump(2);
}

void DBBatchWriter::save_messages_file() {
    json j;
    j["next_id"] = file_next_msg_id_;
    json arr = json::array();
    for (auto& m : file_messages_) {
        arr.push_back({{"id", m.id}, {"from_uid", m.from_uid},
            {"to_uid", m.to_uid}, {"content", m.content}, {"delivered", m.delivered}});
    }
    j["messages"] = arr;
    std::ofstream f(file_data_dir_ + "/messages.json");
    if (f.is_open()) f << j.dump(2);
}

std::string DBBatchWriter::file_hash(const std::string& input) {
    std::hash<std::string> hasher;
    size_t h = hasher("im_salt_" + input);
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(16) << h;
    return ss.str();
}

void DBBatchWriter::loop() {
    std::vector<DBChatMessage> batch;
    while (running_) {
        std::unique_lock<std::mutex> l(mtx_);
        cv_.wait_for(l, std::chrono::milliseconds(500), [this]{ return !queue_.empty() || !running_; });
        while (!queue_.empty() && batch.size() < 100) {
            batch.push_back(std::move(queue_.front()));
            queue_.pop();
        }
        l.unlock();
        if (!batch.empty()) {
            if (use_file_storage_) {
                std::lock_guard<std::mutex> fl(file_mutex_);
                for (auto& m : batch) {
                    file_messages_.push_back({file_next_msg_id_++, m.from_uid, m.to_uid,
                        m.content, m.delivered});
                }
                save_messages_file();
                spdlog::info("File: Saved {} messages", batch.size());
            } else if (ensure_connected()) {
                std::string sql = "INSERT INTO messages (from_uid,to_uid,content,delivered) VALUES ";
                for (size_t i = 0; i < batch.size(); ++i) {
                    if (i > 0) sql += ",";
                    sql += "(" + std::to_string(batch[i].from_uid) + ","
                         + std::to_string(batch[i].to_uid) + ",'"
                         + esc(batch[i].content) + "',"
                         + (batch[i].delivered ? "1" : "0") + ")";
                }
                if (mysql_query(mysql_, sql.c_str())) {
                    spdlog::error("Batch insert failed: {}", mysql_error(mysql_));
                } else {
                    spdlog::info("Batch inserted {} messages to DB", batch.size());
                }
            }
            batch.clear();
        }
    }
}
