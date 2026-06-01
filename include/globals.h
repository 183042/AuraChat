#pragma once
#include <memory>
#include <atomic>

class ThreadPool;
class DBBatchWriter;
class StreamingAIManager;

extern std::unique_ptr<ThreadPool> g_workers;
extern std::unique_ptr<DBBatchWriter> g_db_writer;
extern std::unique_ptr<StreamingAIManager> g_streaming_ai;
extern std::atomic<int> g_conn_count;
extern std::atomic<bool> g_running;
