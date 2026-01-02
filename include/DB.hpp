#ifndef LSMDB_DB_HPP
#define LSMDB_DB_HPP

#include <memory>
#include <string>
#include <optional>
#include <filesystem>

namespace lsmdb {

class DB {
protected:
    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

public:
    static std::unique_ptr<DB> open(const std::filesystem::path& path);

    virtual ~DB() = default;

    virtual void remove(const std::string& key) = 0;
    virtual void put(const std::string& key, const std::string& value) = 0;
    virtual std::optional<std::string> get(const std::string& key) = 0;
};

}

#endif