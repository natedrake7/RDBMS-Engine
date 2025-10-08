cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DatabaseInterface)

set(BUILD_SHARED_LIBS ON)

add_executable(DatabaseInterface main.cpp)

# Set this to the directory containing antlr4-runtimeConfig.cmake or antlr4-runtime-config.cmake
find_package(antlr4-runtime CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# === ANTLR Setup ===
set(ANTLR_JAR_PATH "$ENV{HOME}/antlr/antlr-4.13.1-complete.jar")
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated/antlr)
file(MAKE_DIRECTORY ${GENERATED_DIR})

add_custom_command(
        OUTPUT ${GENERATED_DIR}/SQLParser.cpp
        COMMAND java -Xmx500M -cp ${ANTLR_JAR_PATH} org.antlr.v4.Tool
        -Dlanguage=Cpp -visitor
        -o ${GENERATED_DIR}
        ${CMAKE_SOURCE_DIR}/src/QueryPipeline/Grammar/SQL.g4
        DEPENDS ${CMAKE_SOURCE_DIR}/src/QueryPipeline/Grammar/SQL.g4
        COMMENT "Generating ANTLR C++ parser for SQL.g4"
)

add_custom_target(
        GenerateSQLParser
        DEPENDS ${GENERATED_DIR}/SQLParser.cpp
)

file(GLOB ANTLR_SRC "${GENERATED_DIR}/*.cpp")

add_library(AdditionalLibraries SHARED
        src/AdditionalLibraries/BitMap/BitMap.cpp
        src/AdditionalLibraries/BitMap/BitMap.h
        src/AdditionalLibraries/ByteMap/ByteMap.cpp
        src/AdditionalLibraries/ByteMap/ByteMap.h
        src/AdditionalLibraries/DataTypes/Value/Value.cpp
        src/AdditionalLibraries/DataTypes/Value/Value.h
        src/AdditionalLibraries/DataTypes/DateTime/DateTime.cpp
        src/AdditionalLibraries/DataTypes/DateTime/DateTime.h
        src/AdditionalLibraries/DataTypes/Decimal/Decimal.cpp
        src/AdditionalLibraries/DataTypes/Decimal/Decimal.h
        src/AdditionalLibraries/Converter/Converter.h
        src/AdditionalLibraries/HashSet/HashSet.h
        src/AdditionalLibraries/DataTypes/SortCondition/SortCondition.cpp
        src/AdditionalLibraries/DataTypes/GroupCondition/GroupCondition.cpp
        src/AdditionalLibraries/DataTypes/GroupCondition/GroupCondition.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.cpp
        src/AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.cpp
        src/AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeResponseProtocol.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.cpp
        src/AdditionalLibraries/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.cpp
        src/AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h
        src/AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.cpp
        src/AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h
        src/AdditionalLibraries/DataTypes/Headers/Headers.h
        src/AdditionalLibraries/Functions/StringFunctions.h
        src/AdditionalLibraries/DataTypes/ErrorHandling.h
        src/AdditionalLibraries/DataTypes/Guid/Guid.cpp
        src/AdditionalLibraries/DataTypes/Guid/Guid.h
        src/AdditionalLibraries/Expressions/Expression.h
        src/AdditionalLibraries/Expressions/Expression.cpp
        src/AdditionalLibraries/Functions/DateFunctions.h
        src/AdditionalLibraries/Expressions/Expressions.Additional.h
        src/AdditionalLibraries/Coercions/Coercions.h
        src/AdditionalLibraries/Coercions/Coercions.cpp
        src/AdditionalLibraries/Functions/FunctionSizeEvaluator.h
        src/AdditionalLibraries/Functions/FunctionSizeEvaluator.cpp
)

add_library(QueryPipeline  SHARED
        src/QueryPipeline/Parser/Parser.cpp
        src/QueryPipeline/Parser/Parser.h
        src/QueryPipeline/Visitor/Visitor.cpp
        src/QueryPipeline/Visitor/Visitor.CreateTable.cpp
        src/QueryPipeline/Visitor/Visitor.Joins.cpp
        src/QueryPipeline/Visitor/Visitor.h
        src/QueryPipeline/Cursor/Cursor.cpp
        src/QueryPipeline/Cursor/Cursor.h
        src/QueryPipeline/LogicalPlan/LogicalPlan.cpp
        src/QueryPipeline/LogicalPlan/LogicalPlan.h
        src/QueryPipeline/PhysicalPlan/PhysicalPlan.cpp
        src/QueryPipeline/PhysicalPlan/PhysicalPlan.AlterTable.cpp
        src/QueryPipeline/PhysicalPlan/PhysicalPlan.Joins.cpp
        src/QueryPipeline/PhysicalPlan/PhysicalPlan.h
        src/QueryPipeline/Statements/Statements.cpp
        src/QueryPipeline/Statements/Statements.h
        src/QueryPipeline/PhysicalPlan/PhysicalPlan.cpp
        src/QueryPipeline/ErrorListener/ErrorListener.cpp
        src/QueryPipeline/ErrorListener/ErrorListener.h
        src/QueryPipeline/Constants.h
                ${ANTLR_SRC}
        src/QueryPipeline/QueryResult/QueryResult.cpp
        src/QueryPipeline/QueryResult/QueryResult.h)

add_dependencies(QueryPipeline GenerateSQLParser)

target_link_libraries(QueryPipeline PRIVATE antlr4_static PRIVATE AdditionalLibraries)

# Include the generated parser and lexer files
target_include_directories(QueryPipeline PRIVATE
        ${GENERATED_DIR}
)

add_library(Database SHARED
        src/Database/Database.cpp
        src/Database/Database.IndexHandlers.cpp
        src/Database/Database.h
        src/Database/Table/Table.cpp
        src/Database/Table/Table.AdditionalFunctions.cpp
        src/Database/Table/Table.Indexes.cpp
        src/Database/Table/Table.LargeOjects.cpp
        src/Database/Table/Table.h
        src/Database/Row/Row.cpp
        src/Database/Row/Row.h
        src/Database/Block/Block.cpp
        src/Database/Block/Block.h
        src/Database/Column/Column.cpp
        src/Database/Column/Column.h
        src/Database/Constants.h
        src/Database/Pages/Header/HeaderPage.cpp
        src/Database/Pages/Header/HeaderPage.h
        src/Database/Pages/LargeObject/LargeObjectPage.cpp
        src/Database/Pages/LargeObject/LargeObjectPage.h
        src/Database/Pages/IndexMapAllocation/IndexAllocationMapPage.cpp
        src/Database/Pages/IndexMapAllocation/IndexAllocationMapPage.h
        src/Database/Pages/PageFreeSpace/PageFreeSpacePage.cpp
        src/Database/Pages/PageFreeSpace/PageFreeSpacePage.h
        src/Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.cpp
        src/Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.h
        src/Database/Pages/IndexPage/IndexPage.cpp
        src/Database/Pages/IndexPage/IndexPage.h
        src/Database/Pages/Page.cpp
        src/Database/Pages/Page.h
        src/Database/Storage/FileManager/FileManager.cpp
        src/Database/Storage/FileManager/FileManager.h
        src/Database/Storage/StorageManager/StorageManager.cpp
        src/Database/Storage/StorageManager/StorageManager.h
        src/Database/B+Tree/BPlusTree.cpp
        src/Database/B+Tree/BPlusTree.h
        src/Database/AdditionalFunctions/SortingFunctions.cpp
        src/Database/AdditionalFunctions/SortingFunctions.h
        src/Database/AdditionalFunctions/AggregateFunctions/AggregateFunctions.cpp
        src/Database/AdditionalFunctions/AggregateFunctions/AggregateFunctions.h
        src/Database/AdditionalFunctions/SortingAlgorithms/QuickSort/QuickSort.cpp
        src/Database/AdditionalFunctions/SortingAlgorithms/QuickSort/QuickSort.h
        src/Database/AdditionalFunctions/SortingAlgorithms/MergeSort/MergeSort.cpp
        src/Database/AdditionalFunctions/SortingAlgorithms/MergeSort/MergeSort.h
        src/Database/LockManager/LockManager.cpp
        src/Database/LockManager/LockManager.h
        src/Database/Pages/OverflowPage/OverflowPage.cpp
        src/Database/Pages/OverflowPage/OverflowPage.h
        src/Database/Logger/Logger.cpp
        src/Database/Logger/Logger.h
        src/Database/Logger/Logger.Structures.h
        src/Database/Logger/Logger.Structures.cpp
        src/Database/Logger/WriteAheadLogger/WriteAheadLogger.cpp
        src/Database/Logger/WriteAheadLogger/WriteAheadLogger.h
        src/Database/Logger/CrashRecoveryLogger/CrashRecoveryLogger.cpp
        src/Database/Logger/CrashRecoveryLogger/CrashRecoveryLogger.h
        src/Database/ExpressionExtensions/ExpressionExtensions.cpp
)

add_library(Server SHARED
        src/Server/ConnectionManager/ConnectionManager.cpp
        src/Server/ConnectionManager/ConnectionManager.h
        src/Server/Threadpool/ThreadPool.cpp
        src/Server/Threadpool/ThreadPool.h
        src/Server/Server.cpp
        src/Server/Server.h
        src/Server/MasterDbColumns.h)

target_link_libraries(Server PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(Server PRIVATE AdditionalLibraries)

# add_executable(Client
#         src/Client/client.cpp
#         src/Client/client.h)

# target_link_libraries(Client PRIVATE AdditionalLibraries)

target_link_libraries(DatabaseInterface 
                        PRIVATE Server Database AdditionalLibraries QueryPipeline)
