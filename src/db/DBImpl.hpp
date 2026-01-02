#ifndef LSMDB_DBIMPL_HPP
#define LSMDB_DBIMPL_HPP

#include "DB.hpp"
#include <filesystem>
#include <memory>

namespace lsmdb {

class MemTable;
class WAL;

class DBImpl : public DB {
private:
    std::unique_ptr<MemTable> memTable_;
    std::unique_ptr<WAL> wal_;
    std::filesystem::path path_;
    
    void recoverFromWAL();

public:
    explicit DBImpl(const std::filesystem::path& path);
    ~DBImpl() override;

    void remove(const std::string& key) override;
    void put(const std::string& key, const std::string& value) override;
    std::optional<std::string> get(const std::string& key) override;
};

}

#endif