enable_testing()

add_executable(basic_lsmdb_test BasicLSMDBTest.cpp)
target_link_libraries(basic_lsmdb_test PRIVATE
    lsmdb_db 
    lsmdb_memtable
    lsmdb_sstable
    lsmdb_skiplist
    lsmdb_wal
)
target_include_directories(basic_lsmdb_test PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_test(NAME BasicTests COMMAND basic_lsmdb_test)