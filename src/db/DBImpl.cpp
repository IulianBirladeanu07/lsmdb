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
    loadExistingSSTables();
}

DBImpl::~DBImpl() = default;

void DBImpl::loadExistingSSTables() {
    for (const auto& entry : std::filesystem::directory_iterator(path_)) {
        if (entry.path().extension() == ".sst") {
            auto sstable = std::make_unique<SSTable>(entry.path());
            sstables_.push_back(std::move(sstable));
            
            std::string filename = entry.path().stem().string();
            if (filename.find("sstable_") == 0) {
                uint64_t id = std::stoull(filename.substr(8));
                if (id >= nextSSTableId_) {
                    nextSSTableId_ = id + 1;
                }
            }
        }
    }
}

void DBImpl::flush() {
    std::lock_guard<std::mutex> lock(flushMutex_);
    
    if (memTable_->getSize() == 0) {
        return;
    }
    
    auto sstablePath = path_ / ("sstable_" + std::to_string(nextSSTableId_++) + ".sst");
    
    std::vector<SSTableEntry> entries;
    auto* skiplist = memTable_->getSkipList();
    auto* node = skiplist->getHead()->forward[0].load(std::memory_order_acquire);
    
    while (node) {
        if (!node->key.empty()) {
            entries.push_back({node->key, node->value, node->deleted});
        }
        node = node->forward[0].load(std::memory_order_acquire);
    }
    
    SSTable::create(sstablePath, entries);
    sstables_.push_back(std::make_unique<SSTable>(sstablePath));
    
    memTable_ = std::make_unique<MemTable>();
    wal_->clear();
}

void DBImpl::shouldFlush() {
    if (memTable_->getSize() >= MEMTABLE_FLUSH_THRESHOLD) {
        flush();
    }
}

void DBImpl::recoverFromWAL() {
    auto records = wal_->recover();
    for (const auto& record : records) {
        if (record.type == RecordType::PUT) {
            memTable_->put(record.key, record.value);
        } else if (record.type == RecordType::DELETE) {
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
    auto result = memTable_->get(key);
    if (result.has_value() || memTable_->isDeleted(key)) {
        return result;
    }
    
    for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
        auto value = (*it)->get(key);
        if (value.has_value()) {
            return value;
        }
    }
    
    return std::nullopt;
}

}