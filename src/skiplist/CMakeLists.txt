add_library(lsmdb_skiplist OBJECT
    SkipList.cpp
)

target_include_directories(lsmdb_skiplist 
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
)