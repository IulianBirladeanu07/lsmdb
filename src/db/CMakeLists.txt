add_library(lsmdb_db OBJECT DBImpl.cpp)

target_include_directories(lsmdb_db
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src        
)