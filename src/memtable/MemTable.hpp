#ifndef LSMDB_MEMTABLE_HPP
#define LSMDB_MEMTABLE_HPP

#include "../skiplist/SkipList.hpp"

#include <memory>
#include <string>
#include <optional>
#include <atomic>

namespace lsmdb {

class SkipList;

class MemTable {
private:
    std::unique_ptr<SkipList> skiplist_;
    std::atomic<size_t> size_;
    
public:
    MemTable();
    ~MemTable();
    
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void remove(const std::string& key);    
    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;

    size_t size() const;
    bool shouldFlush(size_t threshold) const;
};

}

#endif