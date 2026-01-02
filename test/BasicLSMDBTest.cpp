#include "db/DBImpl.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>

using namespace lsmdb;

void testBasicOperations() {
    std::cout << "Testing basic operations...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_basic";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");
        
        auto val1 = db.get("key1");
        auto val2 = db.get("key2");
        auto val3 = db.get("key3");
        
        assert(val1.has_value() && val1.value() == "value1");
        assert(val2.has_value() && val2.value() == "value2");
        assert(val3.has_value() && val3.value() == "value3");
        
        auto missing = db.get("nonexistent");
        assert(!missing.has_value());
        
        std::cout << "  Put/Get works\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testUpdate() {
    std::cout << "Testing updates...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_update";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        db.put("key", "value1");
        auto v1 = db.get("key");
        assert(v1.has_value() && v1.value() == "value1");
        
        db.put("key", "value2");
        auto v2 = db.get("key");
        assert(v2.has_value() && v2.value() == "value2");
        
        db.put("key", "value3");
        auto v3 = db.get("key");
        assert(v3.has_value() && v3.value() == "value3");
        
        std::cout << "  Updates work\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testDelete() {
    std::cout << "Testing deletes...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_delete";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        db.put("key1", "value1");
        db.put("key2", "value2");
        
        auto v1 = db.get("key1");
        assert(v1.has_value());
        
        db.remove("key1");
        auto v1_deleted = db.get("key1");
        assert(!v1_deleted.has_value());
        
        auto v2 = db.get("key2");
        assert(v2.has_value() && v2.value() == "value2");
        
        db.remove("nonexistent");
        
        std::cout << "  Deletes work\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testWALRecovery() {
    std::cout << "Testing WAL recovery...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_recovery";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        db.put("persistent1", "value1");
        db.put("persistent2", "value2");
        db.put("persistent3", "value3");
        db.remove("persistent2");
    }
    
    {
        DBImpl db(dbPath);
        
        auto v1 = db.get("persistent1");
        auto v2 = db.get("persistent2");
        auto v3 = db.get("persistent3");
        
        assert(v1.has_value() && v1.value() == "value1");
        assert(!v2.has_value());
        assert(v3.has_value() && v3.value() == "value3");
        
        std::cout << "  WAL recovery works\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testLargeKeys() {
    std::cout << "Testing large keys/values...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_large";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        std::string largeKey(1000, 'k');
        std::string largeValue(10000, 'v');
        
        db.put(largeKey, largeValue);
        auto retrieved = db.get(largeKey);
        
        assert(retrieved.has_value());
        assert(retrieved.value() == largeValue);
        
        std::cout << "  Large keys/values work\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testManyOperations() {
    std::cout << "Testing many operations...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_many";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        for(int i = 0; i < 1000; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            db.put(key, value);
        }
        
        for(int i = 0; i < 1000; i++) {
            std::string key = "key" + std::to_string(i);
            std::string expectedValue = "value" + std::to_string(i);
            auto retrieved = db.get(key);
            assert(retrieved.has_value());
            assert(retrieved.value() == expectedValue);
        }
        
        for(int i = 0; i < 500; i++) {
            std::string key = "key" + std::to_string(i);
            db.remove(key);
        }
        
        for(int i = 0; i < 500; i++) {
            std::string key = "key" + std::to_string(i);
            auto retrieved = db.get(key);
            assert(!retrieved.has_value());
        }
        
        for(int i = 500; i < 1000; i++) {
            std::string key = "key" + std::to_string(i);
            auto retrieved = db.get(key);
            assert(retrieved.has_value());
        }
        
        std::cout << "  Many operations work\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

void testRecoveryAfterManyOps() {
    std::cout << "Testing recovery after many ops...\n";
    
    std::filesystem::path dbPath = "/tmp/test_db_recovery_many";
    std::filesystem::remove_all(dbPath);
    
    {
        DBImpl db(dbPath);
        
        for(int i = 0; i < 100; i++) {
            db.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        
        for(int i = 0; i < 50; i++) {
            db.remove("key" + std::to_string(i));
        }
    }
    
    {
        DBImpl db(dbPath);
        
        for(int i = 0; i < 50; i++) {
            auto v = db.get("key" + std::to_string(i));
            assert(!v.has_value());
        }
        
        for(int i = 50; i < 100; i++) {
            auto v = db.get("key" + std::to_string(i));
            assert(v.has_value());
            assert(v.value() == "value" + std::to_string(i));
        }
        
        std::cout << "  Recovery after many ops works\n";
    }
    
    std::filesystem::remove_all(dbPath);
}

int main() {
    std::cout << "Running LSM-DB tests...\n\n";
    
    try {
        testBasicOperations();
        testUpdate();
        testDelete();
        testWALRecovery();
        testLargeKeys();
        testManyOperations();
        testRecoveryAfterManyOps();
        
        std::cout << "\nAll tests passed\n";
        return 0;
    } catch(const std::exception& e) {
        std::cerr << "\nTest failed: " << e.what() << "\n";
        return 1;
    }
}