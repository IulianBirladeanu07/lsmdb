add_library(lsmdb_wal OBJECT
    Wal.cpp
)
target_include_directories(lsmdb_wal PUBLIC ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(lsmdb_db PRIVATE lsmdb_memtable lsmdb_wal)