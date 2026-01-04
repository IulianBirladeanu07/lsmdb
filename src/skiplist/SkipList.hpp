#ifndef LSMDB_SKIPLIST_HPP
#define LSMDB_SKIPLIST_HPP

#include <string>
#include <atomic>
#include <random>
#include <optional>

namespace lsmdb {

class SkipList {
public:
    struct Node {
        const std::string key;
        std::string value;
        bool deleted;
        const int height;
        std::atomic<Node*> forward[1];

        Node(std::string k, std::string v, int h, bool del = false);
    };

private:
    static constexpr int MAX_HEIGHT = 12;
    static constexpr double PROBABILITY = 0.25;

    Node* head_;
    std::atomic<int> maxHeight_;
    thread_local static std::mt19937 rng_;

    Node* newNode(std::string key, std::string value, int height, bool deleted = false);
    Node* findGreaterOrEqual(const std::string& key, Node** prev) const; 
    int randomHeight();

public:
    SkipList();
    ~SkipList();

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    void remove(const std::string& key);
    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool isDeleted(const std::string& key) const;

    size_t estimateMemoryUsage() const;
    
    Node* getHead() const { return head_; }
};

}

#endif