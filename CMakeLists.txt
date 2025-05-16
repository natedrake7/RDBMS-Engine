cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DatabaseInterface)

add_executable(DatabaseInterface main.cpp)
find_package(antlr4-runtime CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# === ANTLR Setup ===
set(ANTLR_JAR_PATH "$ENV{HOME}/antlr/antlr-4.13.0-complete.jar")
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated/antlr)
file(MAKE_DIRECTORY ${GENERATED_DIR})

add_custom_command(
        OUTPUT ${GENERATED_DIR}/SQLParser.cpp
        COMMAND java -Xmx500M -cp ${ANTLR_JAR_PATH} org.antlr.v4.Tool
        -Dlanguage=Cpp -visitor
        -o ${GENERATED_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/QueryPipeline/Grammar/SQL.g4
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/QueryPipeline/Grammar/SQL.g4
        COMMENT "Generating ANTLR C++ parser for SQL.g4"
)

add_custom_target(
        GenerateSQLParser
        DEPENDS ${GENERATED_DIR}/SQLParser.cpp
)

file(GLOB ANTLR_SRC "${GENERATED_DIR}/*.cpp")

add_library(QueryPipeline   QueryPipeline/Parser/Parser.cpp
                            QueryPipeline/Parser/Parser.h
                            QueryPipeline/Visitor.h
                            ${ANTLR_SRC}
        QueryPipeline/Validator/Validator.cpp
        QueryPipeline/Validator/Validator.h)

add_dependencies(QueryPipeline GenerateSQLParser)

target_link_libraries(QueryPipeline PRIVATE antlr4_static)

# Include the generated parser and lexer files
target_include_directories(QueryPipeline PRIVATE
        ${GENERATED_DIR}
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

add_library(Database    Database/Database.cpp
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
                    Server/Threadpool/ThreadPool.h
                    Server/Server.cpp
                    Server/Server.h)

target_link_libraries(Server PRIVATE nlohmann_json::nlohmann_json)

add_executable(Client   Client/client.cpp
                        Client/client.h)

target_link_libraries(Client PRIVATE AdditionalLibraries)

target_link_libraries(DatabaseInterface 
                        PRIVATE Database Server AdditionalLibraries QueryPipeline)
