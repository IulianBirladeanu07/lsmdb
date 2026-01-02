add_library(lsmdb_memtable OBJECT 
    MemTable.cpp
)

target_include_directories(lsmdb_memtable
    PRIVATE
        ${CMAKE_SORUCE_DIR}/src
)