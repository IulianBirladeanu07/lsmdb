add_subdirectory(db)
add_subdirectory(memtable)
add_subdirectory(wal)
add_subdirectory(sstable)
add_subdirectory(skiplist)

add_library(lsmdb STATIC
    $<TARGET_OBJECTS:lsmdb_db>
    $<TARGET_OBJECTS:lsmdb_memtable>
    $<TARGET_OBJECTS:lsmdb_wal>
    $<TARGET_OBJECTS:lsmdb_sstable>
    $<TARGET_OBJECTS:lsmdb_skiplist>
)

target_include_directories(lsmdb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)