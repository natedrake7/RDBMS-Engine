cmake_minimum_required(VERSION 3.29)
project(DatabaseInterface)

set(CMAKE_CXX_STANDARD 26)

add_executable(DatabaseInterface
        main.cpp)

add_library(Database
        Database/Database.cpp
        Database/Database.h
        Database/Table/Table.cpp
        Database/Table/ComplementaryFunctions.cpp
        Database/Table/IndexesFunctions.cpp
        Database/Table/LargeObjectFunctions.cpp
        Database/Table/Table.h
        Database/Row/Row.cpp
        Database/Row/Row.h
        Database/Block/Block.cpp
        Database/Block/Block.h
        Database/Column/Column.cpp
        Database/Column/Column.h
        Database/Constants.h
        Database/Pages/Header/HeaderPage.cpp
        Database/Pages/Header/HeaderPage.h
        Database/Pages/LargeObject/LargeDataPage.cpp
        Database/Pages/LargeObject/LargeDataPage.h
        Database/Pages/IndexMapAllocation/IndexAllocationMapPage.cpp
        Database/Pages/IndexMapAllocation/IndexAllocationMapPage.h
        Database/Pages/PageFreeSpace/PageFreeSpacePage.cpp
        Database/Pages/PageFreeSpace/PageFreeSpacePage.h
        Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.cpp
        Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.h
        Database/Pages/IndexPage/IndexPage.cpp
        Database/Pages/IndexPage/IndexPage.h
        Database/Pages/Page.cpp
        Database/Pages/Page.h
        AdditionalLibraries/SafeConverter/SafeConverter.cpp
        AdditionalLibraries/SafeConverter/SafeConverter.h
        Database/Storage/FileManager/FileManager.cpp
        Database/Storage/FileManager/FileManager.h
        Database/Storage/StorageManager/StorageManager.cpp
        Database/Storage/StorageManager/StorageManager.h
        AdditionalLibraries/BitMap/BitMap.cpp
        AdditionalLibraries/BitMap/BitMap.h
        AdditionalLibraries/AdditionalObjects/Field/Field.cpp
        AdditionalLibraries/AdditionalObjects/Field/Field.h
        AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.cpp
        AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.h
        AdditionalLibraries/ByteMap/ByteMap.cpp
        AdditionalLibraries/ByteMap/ByteMap.h
        AdditionalLibraries/AdditionalObjects/DateTime/DateTime.cpp
        AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h
        AdditionalLibraries/AdditionalObjects/Decimal/Decimal.cpp
        AdditionalLibraries/AdditionalObjects/Decimal/Decimal.h
        AdditionalLibraries/B+Tree/BPlusTree.cpp
        AdditionalLibraries/B+Tree/BPlusTree.h)

target_link_libraries(DatabaseInterface PRIVATE Database)
