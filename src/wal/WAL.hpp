#ifndef LSMDB_WAL_HPP
#define LSMDB_WAL_HPP

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace lsmdb {

enum class RecordType : uint8_t {
    PUT = 1,
    DELETE = 2
};

struct WalRecord {
    RecordType type;
    std::string key;
    std::string value;
};

class WAL {
private:
    std::filesystem::path path_;
    std::ofstream file_;
    size_t fileSize_;

    void writeRecord(RecordType type, const std::string& key, const std::string& value);

public:
    explicit WAL(const std::filesystem::path& path);
    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    void logPut(const std::string& key, const std::string& value);
    void logDelete(const std::string& key);
    void sync();
    void clear();

    std::vector<WalRecord> recover();
    size_t size() const;
};

}

#endif