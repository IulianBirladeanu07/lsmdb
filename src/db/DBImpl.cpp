#include "DBImpl.hpp"
#include "memtable/MemTable.hpp"
#include "sstable/SSTable.hpp"
#include "wal/WAL.hpp"
#include <filesystem>

namespace lsmdb {

DBImpl::DBImpl(const std::filesystem::path& path) : path_(path), nextSSTableId_(1) {
    std::filesystem::create_directories(path_);

    auto walPath = path_ / "wal.log";
    wal_ = std::make_unique<WAL>(walPath);
    memTable_ = std::make_unique<MemTable>();

    recoverFromWAL();
}

DBImpl::~DBImpl() = default;

void DBImpl::shouldFlush() {
    if(memTable_->getSize() >= MEMTABLE_FLUSH_THRESHOLD);
}

void DBImpl::recoverFromWAL() {
    auto records = wal_->recover();
    for(const auto& record: records) {
        if(record.type == RecordType::PUT) {
            memTable_->put(record.key, record.value);
        } else if(record.type == RecordType::DELETE) {
            memTable_->remove(record.key);
        }
    }
}

void DBImpl::remove(const std::string& key) {
    wal_->logDelete(key);
    wal_->sync();
    memTable_->remove(key);
    shouldFlush();
}

void DBImpl::put(const std::string& key, const std::string& value) {
    wal_->logPut(key, value);
    wal_->sync();
    memTable_->put(key, value);
    shouldFlush();
}

std::optional<std::string> DBImpl::get(const std::string& key) {
    return memTable_->get(key);
}

}