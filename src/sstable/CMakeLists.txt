add_library(lsmdb_sstable OBJECT 
    SSTable.cpp
)

target_include_directories(lsmdb_sstable
    PRIVATE
        ${CMAKE_SORUCE_DIR}/src
)