#include "business.h"
#include "config.h"
#include "globals.h"
#include "worker.h"
#include "session_manager.h"
#include "db_writer.h"
#include "ai_manager.h"
#include "message.pb.h"
#include "spdlog/spdlog.h"
#include <chrono>

using namespace my_chat;

void process_business(std::shared_ptr<ClientContext> ctx, PacketHeader hdr, std::string body) {
    try {
        if (hdr.cmd == (uint32_t)CmdType::CHAT) {
            int64_t now_sec = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            int window = Config::RATE_LIMIT_WINDOW();
            if (ctx->rate_window_start == 0 || now_sec - ctx->rate_window_start > window) {
                ctx->rate_window_start = now_sec;
                ctx->rate_msg_count = 0;
            }
            if (++ctx->rate_msg_count > Config::RATE_LIMIT_MSGS()) {
                Response rl;
                rl.set_success(false);
                rl.set_msg("消息发送过快，请稍后再试");
                std::string out;
                rl.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                return;
            }
        }

        if (hdr.cmd == (uint32_t)CmdType::LOGIN) {
            LoginRequest req;
            if (req.ParseFromString(body)) {
                int uid = g_db_writer ? g_db_writer->verify_login(req.username(), req.password()) : -1;
                if (uid > 0) {
                    SessionManager::instance().bind(uid, ctx);
                    spdlog::info("User {} logged in (uid={})", req.username(), uid);

                    if (g_db_writer) {
                        auto offline_msgs = g_db_writer->query_offline_messages(uid);
                        if (!offline_msgs.empty()) {
                            spdlog::info("Delivering {} offline messages to uid {}", offline_msgs.size(), uid);
                            for (auto& m : offline_msgs) {
                                ChatMessage cm;
                                cm.set_from_uid(m.from_uid);
                                cm.set_to_uid(m.to_uid);
                                cm.set_content(m.content);
                                cm.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count());
                                std::string cm_out;
                                cm.SerializeToString(&cm_out);
                                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::CHAT, 0, cm_out);
                            }
                            g_db_writer->mark_delivered(uid, INT64_MAX);
                        }
                    }

                    Response resp;
                    resp.set_success(true);
                    resp.set_uid(uid);
                    resp.set_msg("login success");
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                } else {
                    Response resp;
                    resp.set_success(false);
                    resp.set_msg("用户名或密码错误");
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                    spdlog::warn("Login failed for user {}", req.username());
                }
            }
        } else if (hdr.cmd == (uint32_t)CmdType::CHAT) {
            ChatMessage msg;
            if (msg.ParseFromString(body)) {
                if (msg.to_uid() == 9999) {
                    int sender_uid = ctx->uid.load();
                    std::string prompt = msg.content();
                    spdlog::info("Stream AI request from uid {}: {}", sender_uid, prompt.substr(0, 50));

                    if (!g_streaming_ai->submit_stream_request(sender_uid, prompt, ctx)) {
                        Response error_resp;
                        error_resp.set_success(false);
                        error_resp.set_uid(sender_uid);
                        error_resp.set_msg("AI服务繁忙，请稍后再试");
                        std::string out;
                        error_resp.SerializeToString(&out);
                        ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                    }
                    return;
                }

                auto target = SessionManager::instance().get(msg.to_uid());
                if (target) {
                    msg.set_from_uid(ctx->uid.load());
                    std::string out;
                    msg.SerializeToString(&out);
                    target->owner_worker->queue_send_packet(target, (uint32_t)CmdType::CHAT, 0, out);

                    Response ack;
                    ack.set_success(true);
                    ack.set_uid(ctx->uid.load());
                    ack.set_msg("消息已发送给用户" + std::to_string(msg.to_uid()));
                    std::string ack_out;
                    ack.SerializeToString(&ack_out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, ack_out);

                    spdlog::info("Message from {} to {} delivered", ctx->uid.load(), msg.to_uid());

                    if (g_db_writer) {
                        g_db_writer->submit({0, ctx->uid.load(), msg.to_uid(), msg.content(), std::chrono::system_clock::now(), true});
                    }
                } else {
                    if (g_db_writer) {
                        g_db_writer->submit({0, ctx->uid.load(), msg.to_uid(), msg.content(), std::chrono::system_clock::now(), false});
                    }
                    Response ack;
                    ack.set_success(false);
                    ack.set_uid(ctx->uid.load());
                    ack.set_msg("用户" + std::to_string(msg.to_uid()) + "不在线，消息已存储");
                    std::string ack_out;
                    ack.SerializeToString(&ack_out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, ack_out);

                    spdlog::info("Offline message from {} to {} stored", ctx->uid.load(), msg.to_uid());
                }
            }
        } else if (hdr.cmd == (uint32_t)CmdType::REGISTER) {
            RegisterRequest req;
            if (req.ParseFromString(body)) {
                if (req.username().empty() || req.password().empty()) {
                    Response resp;
                    resp.set_success(false);
                    resp.set_msg("用户名和密码不能为空");
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                } else if (!g_db_writer) {
                    Response resp;
                    resp.set_success(false);
                    resp.set_msg("服务暂不可用");
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                } else {
                    int uid = g_db_writer->register_user(req.username(), req.password());
                    Response resp;
                    if (uid == -2) {
                        resp.set_success(false);
                        resp.set_msg("用户名已存在");
                    } else if (uid < 0) {
                        resp.set_success(false);
                        resp.set_msg("注册失败，请稍后再试");
                    } else {
                        resp.set_success(true);
                        resp.set_uid(uid);
                        resp.set_msg("注册成功，您的UID是" + std::to_string(uid));
                        SessionManager::instance().bind(uid, ctx);
                    }
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                }
            }
        } else if (hdr.cmd == (uint32_t)CmdType::HEARTBEAT) {
            Response pong;
            pong.set_success(true);
            pong.set_uid(ctx->uid.load());
            pong.set_msg("PONG");
            std::string out;
            pong.SerializeToString(&out);
            ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
        } else if (hdr.cmd == (uint32_t)CmdType::ADD_FRIEND) {
            AddFriendRequest req;
            if (req.ParseFromString(body)) {
                int from_uid = ctx->uid.load();
                Response resp;
                resp.set_uid(from_uid);
                if (req.to_uid() == from_uid) {
                    resp.set_success(false);
                    resp.set_msg("不能添加自己为好友");
                } else if (!g_db_writer) {
                    resp.set_success(false);
                    resp.set_msg("服务暂不可用");
                } else {
                    int ret = g_db_writer->add_friend_request(from_uid, req.to_uid(), req.message());
                    if (ret == -2) {
                        resp.set_success(false);
                        resp.set_msg("已经是好友了");
                    } else if (ret == -3) {
                        resp.set_success(false);
                        resp.set_msg("已发送过好友请求，请等待对方处理");
                    } else if (ret < 0) {
                        resp.set_success(false);
                        resp.set_msg("发送失败，请稍后再试");
                    } else {
                        resp.set_success(true);
                        resp.set_msg("好友请求已发送");
                        auto target = SessionManager::instance().get(req.to_uid());
                        if (target) {
                            Response notify;
                            notify.set_success(true);
                            notify.set_msg("收到新的好友请求");
                            std::string nout;
                            notify.SerializeToString(&nout);
                            target->owner_worker->queue_send_packet(target, (uint32_t)CmdType::ACK, 0, nout);
                        }
                    }
                }
                std::string out;
                resp.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
            }
        } else if (hdr.cmd == (uint32_t)CmdType::ACCEPT_FRIEND) {
            AcceptFriendRequest req;
            if (req.ParseFromString(body)) {
                int acceptor_uid = ctx->uid.load();
                Response resp;
                resp.set_uid(acceptor_uid);
                if (!g_db_writer) {
                    resp.set_success(false);
                    resp.set_msg("服务暂不可用");
                } else {
                    int from_uid = g_db_writer->accept_friend_request(req.request_id(), acceptor_uid);
                    if (from_uid < 0) {
                        resp.set_success(false);
                        resp.set_msg("处理失败，请求可能已过期");
                    } else {
                        resp.set_success(true);
                        resp.set_msg("已添加为好友");
                        auto target = SessionManager::instance().get(from_uid);
                        if (target) {
                            Response notify;
                            notify.set_success(true);
                            notify.set_msg("您的好友请求已被接受");
                            std::string nout;
                            notify.SerializeToString(&nout);
                            target->owner_worker->queue_send_packet(target, (uint32_t)CmdType::ACK, 0, nout);
                        }
                    }
                }
                std::string out;
                resp.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
            }
        } else if (hdr.cmd == (uint32_t)CmdType::FRIEND_LIST) {
            if (!g_db_writer) {
                Response resp;
                resp.set_success(false);
                resp.set_msg("服务暂不可用");
                std::string out;
                resp.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
            } else {
                auto friends = g_db_writer->get_friends(ctx->uid.load());
                UserList ul;
                for (auto& [f_uid, f_name] : friends) {
                    auto* info = ul.add_users();
                    info->set_uid(f_uid);
                    info->set_username(f_name);
                    info->set_online(SessionManager::instance().get(f_uid) != nullptr);
                }
                std::string out;
                ul.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::FRIEND_LIST, hdr.seq, out);
            }
        } else if (hdr.cmd == (uint32_t)CmdType::SEARCH_USER) {
            SearchUserRequest req;
            if (req.ParseFromString(body)) {
                if (!g_db_writer) {
                    Response resp;
                    resp.set_success(false);
                    resp.set_msg("服务暂不可用");
                    std::string out;
                    resp.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
                } else {
                    auto users = g_db_writer->search_users(req.keyword());
                    UserList ul;
                    for (auto& [u_uid, u_name] : users) {
                        auto* info = ul.add_users();
                        info->set_uid(u_uid);
                        info->set_username(u_name);
                        info->set_online(SessionManager::instance().get(u_uid) != nullptr);
                    }
                    std::string out;
                    ul.SerializeToString(&out);
                    ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::SEARCH_USER, hdr.seq, out);
                }
            }
        } else if (hdr.cmd == (uint32_t)CmdType::FRIEND_REQUESTS) {
            if (!g_db_writer) {
                Response resp;
                resp.set_success(false);
                resp.set_msg("服务暂不可用");
                std::string out;
                resp.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
            } else {
                auto requests = g_db_writer->get_pending_requests(ctx->uid.load());
                FriendRequestList frl;
                for (auto& [id, from_uid, from_name, msg] : requests) {
                    auto* info = frl.add_requests();
                    info->set_id(id);
                    info->set_from_uid(from_uid);
                    info->set_from_username(from_name);
                    info->set_message(msg);
                }
                std::string out;
                frl.SerializeToString(&out);
                ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::FRIEND_REQUESTS, hdr.seq, out);
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("Business logic error: {}", e.what());
        try {
            Response error_resp;
            error_resp.set_success(false);
            error_resp.set_msg("服务器内部错误");
            std::string out;
            error_resp.SerializeToString(&out);
            ctx->owner_worker->queue_send_packet(ctx, (uint32_t)CmdType::ACK, hdr.seq, out);
        } catch (...) {}
    }
}
