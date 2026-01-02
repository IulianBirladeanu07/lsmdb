#include "SkipList.hpp"

namespace lsmdb {

thread_local std::mt19937 SkipList::rng_(std::random_device{}());

SkipList::Node::Node(std::string k, std::string v, int h, bool del)
    : key(std::move(k))
    , value(std::move(v))
    , deleted(del)
    , height(h) {
    for(int i = 0; i < h; i++) {
        forward[i].store(nullptr, std::memory_order_relaxed);
    }
}
    
SkipList::SkipList() : maxHeight_(1) {
    head_ = newNode("", "", MAX_HEIGHT);
}

SkipList::~SkipList() {
    Node* current = head_;
    while(current) {
        Node* next = current->forward[0].load(std::memory_order_relaxed);
        delete current;
        current = next;
    }
}

SkipList::Node* SkipList::newNode(std::string key, std::string value, int height, bool deleted) {
    size_t nodeSize = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
    void* mem = ::operator new(nodeSize);
    return new (mem) Node(std::move(key), std::move(value), height, deleted);
}

int SkipList::randomHeight() {
    int height = 1;
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    while(height < MAX_HEIGHT && dist(rng_) < PROBABILITY) {
        height++;
    }
    return height;
}

SkipList::Node* SkipList::findGreaterOrEqual(const std::string& key, Node** previous) const {
    Node* current = head_;
    int level = maxHeight_.load(std::memory_order_acquire) - 1;

    while(level >= 0) {
        Node* next = current->forward[level].load(std::memory_order_acquire);

        if(next && next->key < key) {
            current = next;
        } else {
            if(previous) {
                previous[level] = current;
            }
            level--;
        }
    }

    return current->forward[0].load(std::memory_order_acquire);
}

void SkipList::put(const std::string& key, const std::string& value) {
    Node* previous[MAX_HEIGHT];
    Node* current = findGreaterOrEqual(key, previous);

    if(current && current->key == key) {
        current->value = value;
        current->deleted = false;
        return;
    }

    int height = randomHeight();
    int currentMaxHeight = maxHeight_.load(std::memory_order_relaxed);

    if(height > currentMaxHeight) {
        for(int i = currentMaxHeight; i < height; i++) {
            previous[i] = head_;
        }
        maxHeight_.store(height, std::memory_order_release);
    }

    Node* node = newNode(key, value, height);

    for(int level = 0; level < height; level++) {
        Node* next = previous[level]->forward[level].load(std::memory_order_relaxed);
        node->forward[level].store(next, std::memory_order_relaxed);
        previous[level]->forward[level].store(node, std::memory_order_release);
    }
}

std::optional<std::string> SkipList::get(const std::string& key) const {
    Node* node = findGreaterOrEqual(key, nullptr);

    if(node && node->key == key && !node->deleted) {
        return node->value;
    }
    return std::nullopt;
}

void SkipList::remove(const std::string& key) {
    Node* previous[MAX_HEIGHT];
    Node* current = findGreaterOrEqual(key, previous);

    if(current && current->key == key) {
        current->deleted = true;
    }
}

size_t SkipList::estimateMemoryUsage() const {
    size_t total = sizeof(SkipList);
    Node* current = head_;

    while(current) {
        total += sizeof(Node) + sizeof(std::atomic<Node*>) * (current->height - 1);
        total += current->key.capacity() + current->value.capacity();
        current = current->forward[0].load(std::memory_order_relaxed);
    }
    
    return total;
}

}