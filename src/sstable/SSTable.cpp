#include "SSTable.hpp"
#include <fstream>
#include <algorithm>

namespace lsmdb {

SSTable::SSTable(const std::filesystem::path& path) : path_(path) {
    if(std::filesystem::exists(path_)) {
        loadIndex();
    }
}

void SSTable::writeEntry(std::ofstream& file, const std::string& key, const std::string& value, bool deleted) {
    uint8_t deletedFlag = deleted ? 1 : 0;
    uint32_t keySize = key.size();
    uint32_t valueSize = value.size();
    
    file.write(reinterpret_cast<const char*>(&deletedFlag), sizeof(deletedFlag));
    file.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
    file.write(key.data(), keySize);
    file.write(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
    file.write(value.data(), valueSize);
}

void SSTable::create(const std::filesystem::path& path, const std::vector<SSTableEntry>& entries) {
    std::ofstream file(path, std::ios::binary);
    if(!file) {
        throw std::runtime_error("Failed to create SSTable file");
    }
    
    std::vector<SSTableEntry> sorted = entries;
    std::sort(sorted.begin(), sorted.end(), [](const SSTableEntry& a, const SSTableEntry& b) {
        return a.key < b.key;
    });
    
    std::vector<IndexEntry> index;
    for(const auto& entry : sorted) {
        uint64_t offset = file.tellp();
        index.push_back({entry.key, offset});
        
        uint8_t deletedFlag = entry.deleted ? 1 : 0;
        uint32_t keySize = entry.key.size();
        uint32_t valueSize = entry.value.size();
        
        file.write(reinterpret_cast<const char*>(&deletedFlag), sizeof(deletedFlag));
        file.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        file.write(entry.key.data(), keySize);
        file.write(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
        file.write(entry.value.data(), valueSize);
    }
    
    uint32_t indexSize = index.size();
    file.write(reinterpret_cast<const char*>(&indexSize), sizeof(indexSize));
    
    for(const auto& entry : index) {
        uint32_t keySize = entry.key.size();
        file.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        file.write(entry.key.data(), keySize);
        file.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    }
}

void SSTable::loadIndex() {
    std::ifstream file(path_, std::ios::binary);
    if(!file) {
        throw std::runtime_error("Failed to open SSTable file");
    }
    
    file.seekg(-static_cast<std::streamoff>(sizeof(uint32_t)), std::ios::end);
    
    uint32_t indexSize;
    file.read(reinterpret_cast<char*>(&indexSize), sizeof(indexSize));
    
    std::streamoff indexStart = -static_cast<std::streamoff>(
        sizeof(uint32_t) + indexSize * (sizeof(uint32_t) + sizeof(uint64_t))
    );
    
    file.seekg(indexStart, std::ios::end);
    file.seekg(static_cast<std::streamoff>(sizeof(uint32_t)), std::ios::cur);
    
    index_.reserve(indexSize);
    for(uint32_t i = 0; i < indexSize; i++) {
        uint32_t keySize;
        file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        file.read(&key[0], keySize);
        
        uint64_t offset;
        file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        
        index_.push_back({std::move(key), offset});
    }
}

std::optional<std::string> SSTable::get(const std::string& key) const {
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
        [](const IndexEntry& entry, const std::string& k) {
            return entry.key < k;
        });
    
    if(it == index_.end() || it->key != key) {
        return std::nullopt;
    }
    
    std::ifstream file(path_, std::ios::binary);
    if(!file) {
        return std::nullopt;
    }
    
    file.seekg(it->offset);
    
    uint8_t deletedFlag;
    uint32_t keySize;
    uint32_t valueSize;
    
    file.read(reinterpret_cast<char*>(&deletedFlag), sizeof(deletedFlag));
    file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
    
    file.seekg(keySize, std::ios::cur);
    
    file.read(reinterpret_cast<char*>(&valueSize), sizeof(valueSize));
    
    if(deletedFlag) {
        return std::nullopt;
    }
    
    std::string value(valueSize, '\0');
    file.read(&value[0], valueSize);
    
    return value;
}

bool SSTable::contains(const std::string& key) const {
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
        [](const IndexEntry& entry, const std::string& k) {
            return entry.key < k;
        });
    
    return it != index_.end() && it->key == key;
}

std::vector<SSTableEntry> SSTable::readAll() const {
    std::vector<SSTableEntry> entries;
    std::ifstream file(path_, std::ios::binary);
    
    if(!file) {
        return entries;
    }
    
    for(const auto& idx : index_) {
        file.seekg(idx.offset);
        
        uint8_t deletedFlag;
        uint32_t keySize;
        uint32_t valueSize;
        
        file.read(reinterpret_cast<char*>(&deletedFlag), sizeof(deletedFlag));
        file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        file.read(&key[0], keySize);
        
        file.read(reinterpret_cast<char*>(&valueSize), sizeof(valueSize));
        
        std::string value(valueSize, '\0');
        file.read(&value[0], valueSize);
        
        entries.push_back({std::move(key), std::move(value), deletedFlag != 0});
    }
    
    return entries;
}

const std::filesystem::path& SSTable::getPath() const {
    return path_;
}

size_t SSTable::size() const {
    return index_.size();
}

}