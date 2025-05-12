cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DatabaseInterface)

find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

FLEX_TARGET(Lexer   ${CMAKE_CURRENT_SOURCE_DIR}/QueryParser/Grammar/Grammar.l 
                    ${CMAKE_CURRENT_BINARY_DIR}/lexer.cpp)

BISON_TARGET(Parser ${CMAKE_CURRENT_SOURCE_DIR}/QueryParser/Grammar/Grammar.y
                    ${CMAKE_CURRENT_BINARY_DIR}/parser.cpp)

ADD_FLEX_BISON_DEPENDENCY(Lexer Parser)

add_executable(DatabaseInterface main.cpp)

add_library(QueryParser
        QueryParser/Tokenizer/Tokenizer.cpp
        QueryParser/Tokenizer/Tokenizer.h
        QueryParser/Parser/Parser.cpp
        QueryParser/Parser/Parser.h
        QueryParser/Expression.h
        ${CMAKE_CURRENT_BINARY_DIR}
        ${BISON_Parser_OUTPUTS}
        ${FLEX_Lexer_OUTPUTS} 
)


add_library(AdditionalLibraries
            AdditionalLibraries/BitMap/BitMap.cpp
            AdditionalLibraries/BitMap/BitMap.h
            AdditionalLibraries/ByteMap/ByteMap.cpp
            AdditionalLibraries/ByteMap/ByteMap.h
            AdditionalLibraries/AdditionalDataTypes/Field/Field.cpp
            AdditionalLibraries/AdditionalDataTypes/Field/Field.h
            AdditionalLibraries/AdditionalDataTypes/JoinField/JoinField.cpp
            AdditionalLibraries/AdditionalDataTypes/JoinField/JoinField.h
            AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.cpp
            AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h
            AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.cpp
            AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h
            Database/RowCondition/RowCondition.cpp
            Database/RowCondition/RowCondition.h
            AdditionalLibraries/SafeConverter/SafeConverter.cpp
            AdditionalLibraries/SafeConverter/SafeConverter.h
            AdditionalLibraries/HashSet/HashSet.h
            AdditionalLibraries/AdditionalDataTypes/SortCondition/SortCondition.cpp
            AdditionalLibraries/AdditionalDataTypes/GroupCondition/GroupCondition.cpp
            AdditionalLibraries/AdditionalDataTypes/GroupCondition/GroupCondition.h
            AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.cpp
            AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.h
            AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.cpp
            AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h
            AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeResponseProtocol.h
            AdditionalLibraries/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.cpp
            AdditionalLibraries/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.h
            AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.cpp
            AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h
            AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.cpp
            AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h)

add_library(Database
        Database/Database.cpp
        Database/DatabaseIndexHandlers.cpp
        Database/DatabaseJoins.cpp
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
        Database/Storage/FileManager/FileManager.cpp
        Database/Storage/FileManager/FileManager.h
        Database/Storage/StorageManager/StorageManager.cpp
        Database/Storage/StorageManager/StorageManager.h
        Database/B+Tree/BPlusTree.cpp
        Database/B+Tree/BPlusTree.h
        Database/AdditionalFunctions/SortingFunctions.cpp
        Database/AdditionalFunctions/SortingFunctions.h
        Database/AdditionalFunctions/AggregateFunctions/AggregateFunctions.cpp
        Database/AdditionalFunctions/AggregateFunctions/AggregateFunctions.h
        Database/AdditionalFunctions/SortingAlgorithms/QuickSort/QuickSort.cpp
        Database/AdditionalFunctions/SortingAlgorithms/QuickSort/QuickSort.h
        Database/AdditionalFunctions/SortingAlgorithms/MergeSort/MergeSort.cpp
        Database/AdditionalFunctions/SortingAlgorithms/MergeSort/MergeSort.h
        Database/AdditionalFunctions/StringFunctions/StringFunctions.cpp
        Database/AdditionalFunctions/StringFunctions/StringFunctions.h)

add_library(Server  Server/ConnectionManager/ConnectionManager.cpp
                    Server/ConnectionManager/ConnectionManager.h
                    Server/Threadpool/ThreadPool.cpp
                    Server/Threadpool/ThreadPool.h)

add_executable(Client   Client/client.cpp
                        Client/client.h)

target_include_directories(DatabaseInterface PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(Client PRIVATE AdditionalLibraries)

target_link_libraries(DatabaseInterface PRIVATE Database Server AdditionalLibraries QueryParser)
