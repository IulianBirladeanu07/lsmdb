#include "Wal.hpp"
#include <cstring>

namespace lsmdb {

WAL::WAL(const std::filesystem::path& path) 
    : path_(path)
    , fileSize_(0) {
    file_.open(path_, std::ios::binary | std::ios::app);
    if (!file_) {
        throw std::runtime_error("Failed to open WAL file");
    }
    fileSize_ = std::filesystem::file_size(path_);
}

WAL::~WAL() {
    if (file_.is_open()) {
        file_.close();
    }
}

void WAL::writeRecord(RecordType type, const std::string& key, const std::string& value) {
    uint8_t recordType = static_cast<uint8_t>(type);
    uint32_t keySize = key.size();
    uint32_t valueSize = value.size();
    
    file_.write(reinterpret_cast<const char*>(&recordType), sizeof(recordType));
    file_.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
    file_.write(key.data(), keySize);
    file_.write(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
    file_.write(value.data(), valueSize);
    
    fileSize_ += sizeof(recordType) + sizeof(keySize) + keySize + sizeof(valueSize) + valueSize;
}

void WAL::logPut(const std::string& key, const std::string& value) {
    writeRecord(RecordType::PUT, key, value);
}

void WAL::logDelete(const std::string& key) {
    writeRecord(RecordType::DELETE, key, "");
}

void WAL::sync() {
    file_.flush();
    // file_.sync();
}

void WAL::clear() {
    file_.close();
    std::filesystem::remove(path_);
    file_.open(path_, std::ios::binary | std::ios::app);
    fileSize_ = 0;
}

std::vector<WalRecord> WAL::recover() {
    std::vector<WalRecord> records;
    std::ifstream recoveryFile(path_, std::ios::binary);
    
    if (!recoveryFile) {
        return records;
    }
    
    while (recoveryFile.peek() != EOF) {
        uint8_t recordType;
        uint32_t keySize;
        uint32_t valueSize;
        
        recoveryFile.read(reinterpret_cast<char*>(&recordType), sizeof(recordType));
        if (recoveryFile.gcount() != sizeof(recordType)) break;
        
        recoveryFile.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        if (recoveryFile.gcount() != sizeof(keySize)) break;
        
        std::string key(keySize, '\0');
        recoveryFile.read(&key[0], keySize);
        if (recoveryFile.gcount() != static_cast<std::streamsize>(keySize)) break;
        
        recoveryFile.read(reinterpret_cast<char*>(&valueSize), sizeof(valueSize));
        if (recoveryFile.gcount() != sizeof(valueSize)) break;
        
        std::string value(valueSize, '\0');
        recoveryFile.read(&value[0], valueSize);
        if (recoveryFile.gcount() != static_cast<std::streamsize>(valueSize)) break;
        
        records.push_back({
            static_cast<RecordType>(recordType),
            std::move(key),
            std::move(value)
        });
    }
    
    return records;
}

size_t WAL::size() const {
    return fileSize_;
}

}