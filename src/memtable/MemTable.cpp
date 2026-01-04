#include "MemTable.hpp"

namespace lsmdb {

MemTable::MemTable() 
    : skiplist_(std::make_unique<SkipList>())
    , size_(0) {
}

MemTable::~MemTable() = default;

void MemTable::put(const std::string& key, const std::string& value) {
    skiplist_->put(key, value);
    size_.store(skiplist_->estimateMemoryUsage(), std::memory_order_relaxed);
}

std::optional<std::string> MemTable::get(const std::string& key) const {
    return skiplist_->get(key);
}

void MemTable::remove(const std::string& key) {
    skiplist_->remove(key);
    size_.store(skiplist_->estimateMemoryUsage(), std::memory_order_relaxed);
}

size_t MemTable::getSize() const {
    return size_.load(std::memory_order_relaxed);
}

bool MemTable::shouldFlush(size_t threshold) const {
    return getSize() >= threshold;
}

bool MemTable::isDeleted(const std::string& key) const {
    return skiplist_->isDeleted(key);
}

SkipList* MemTable::getSkipList() const {
    return skiplist_.get();
}
}