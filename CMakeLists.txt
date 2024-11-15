cmake_minimum_required(VERSION 3.29)
project(DatabaseInterface)

set(CMAKE_CXX_STANDARD 11)

add_executable(DatabaseInterface
        main.cpp)
add_library(AdditionalLibraries
        AdditionalLibraries/HashTable/HashTable.cpp
        AdditionalLibraries/HashTable/HashTable.h
        AdditionalLibraries/SafeConverter/SafeConverter.cpp
        AdditionalLibraries/SafeConverter/SafeConverter.h
        AdditionalLibraries/FileManager/FileManager.cpp
        AdditionalLibraries/FileManager/FileManager.h)
add_library(Database
        Database/Database.cpp
        Database/Database.h
        Database/Table/Table.cpp
        Database/Table/Table.h
        Database/Row/Row.cpp
        Database/Row/Row.h
        Database/Block/Block.cpp
        Database/Block/Block.h
        Database/Column/Column.cpp
        Database/Column/Column.h)

target_link_libraries(DatabaseInterface PRIVATE Database PRIVATE AdditionalLibraries)
