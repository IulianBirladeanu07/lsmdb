add_library(lsmdb_wal OBJECT 
    WAL.cpp
)

target_include_directories(lsmdb_wal
    PRIVATE
        ${CMAKE_SORUCE_DIR}/src
)