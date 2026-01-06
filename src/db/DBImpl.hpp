#ifndef LSMDB_DBIMPL_HPP
#define LSMDB_DBIMPL_HPP

#include "DB.hpp"

#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

namespace lsmdb {

class MemTable;
class SSTable;
class WAL;

class DBImpl : public DB {
private:
    std::unique_ptr<MemTable> memTable_;
    std::unique_ptr<WAL> wal_;
    std::filesystem::path path_;
    
    std::vector<std::unique_ptr<SSTable>> sstables_;
    std::mutex flushMutex_;

    uint64_t nextSSTableId_;
    static constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 64 * 1024 * 1024;

    void recoverFromWAL();
    void loadExistingSSTables();
    void flush();
    void shouldFlush();

public:
    explicit DBImpl(const std::filesystem::path& path);
    ~DBImpl() override;

    void remove(const std::string& key) override;
    void put(const std::string& key, const std::string& value) override;
    std::optional<std::string> get(const std::string& key) override;
};

}

#endif