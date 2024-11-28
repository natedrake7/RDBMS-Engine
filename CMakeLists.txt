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
        AdditionalLibraries/FileManager/FileManager.h
        AdditionalLibraries/PageManager/PageManager.cpp
        AdditionalLibraries/PageManager/PageManager.h
        AdditionalLibraries/BitMap/BitMap.cpp
        AdditionalLibraries/BitMap/BitMap.h
        AdditionalLibraries/AdditionalObjects/Field/Field.cpp
        AdditionalLibraries/AdditionalObjects/Field/Field.h
        AdditionalLibraries/AdditionalObjects/RowCondition.cpp
        AdditionalLibraries/AdditionalObjects/RowCondition.h
        Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.cpp
        Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.h)
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
        Database/Column/Column.h
        Database/Pages/Page.cpp
        Database/Pages/Page.h
        Database/Constants.h
        Database/Pages/Header/HeaderPage.cpp
        Database/Pages/Header/HeaderPage.h
        Database/Pages/LargeObject/LargeDataPage.cpp
        Database/Pages/LargeObject/LargeDataPage.h
        Database/Pages/IndexMapAllocation/IndexAllocationMapPage.cpp
        Database/Pages/IndexMapAllocation/IndexAllocationMapPage.h)

target_link_libraries(DatabaseInterface PRIVATE Database PRIVATE AdditionalLibraries)
