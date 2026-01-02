#include "DBImpl.hpp"
#include "memtable/MemTable.hpp"
#include <filesystem>

namespace lsmdb {

DBImpl::DBImpl(const std::filesystem::path& path) : path_(path) {
    std::filesystem::create_directories(path_);
    memTable_ = std::make_unique<MemTable>();
}

DBImpl::~DBImpl() = default;

void DBImpl::remove(const std::string& key) {
    memTable_->remove(key);
}

void DBImpl::put(const std::string& key, const std::string& value) {
    memTable_->put(key, value);
}

std::optional<std::string> DBImpl::get(const std::string& key) {
    return memTable_->get(key);
}

}