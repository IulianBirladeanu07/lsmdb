#ifndef LSMDB_SSTABLE_HPP
#define LSMDB_SSTABLE_HPP

#include <filesystem>
#include <string>
#include <vector>
#include <optional>
#include <cstdint>

namespace lsmdb {

struct SSTableEntry {
    std::string key;
    std::string value;
    bool deleted;
};

struct IndexEntry {
    std::string key;
    uint64_t offset;
};

class SSTable {
private:
    std::filesystem::path path_;
    std::vector<IndexEntry> index_;

    void writeEntry(std::ofstream& file, const std::string& key, const std::string& value, bool deleted = false);
    void loadIndex();

public:
    explicit SSTable(const std::filesystem::path& path);

    static void create(const std::filesystem::path& path, const std::vector<SSTableEntry> entries);

    std::optional<std::string> get(const std::string& key) const;
    bool contains(const std::string& key) const;

    std::vector<SSTableEntry> readAll() const;

    const std::filesystem::path& getPath() const;
    size_t size() const;
};
}

#endif // LSMDB_SSTABLE_HPP