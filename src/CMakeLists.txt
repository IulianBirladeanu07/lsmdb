add_subdirectory(db)
add_subdirectory(memtable)
add_subdirectory(wal)
add_subdirectory(sstable)
add_subdirectory(skiplist)

add_library(lsmdb STATIC
    db/DBImpl.cpp
)

target_include_directories(lsmdb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(lsmdb
    PRIVATE
        # lsmdb_memtable
        # lsmdb_wal
        # lsmdb_sstable
)