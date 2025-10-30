cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DatabaseInterface)

set(BUILD_SHARED_LIBS ON)

add_executable(DatabaseInterface main.cpp
        Queries.h)

# Set this to the directory containing antlr4-runtimeConfig.cmake or antlr4-runtime-config.cmake
find_package(antlr4-runtime CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
#find_package(unofficial-sodium CONFIG REQUIRED)
find_package(unofficial-argon2 CONFIG REQUIRED)


# === ANTLR Setup ===
if(WIN32)
    set(ANTLR_JAR_PATH "${CMAKE_SOURCE_DIR}/External/ANTLR/antlr4_complete.jar")
else ()
    set(ANTLR_JAR_PATH "$ENV{HOME}/antlr/antlr-4.13.1-complete.jar")
endif()

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

add_library(Systemic SHARED
        src/Systemic/DataStructures/BitMap/BitMap.cpp
        src/Systemic/DataStructures/BitMap/BitMap.h
        src/Systemic/DataStructures/ByteMap/ByteMap.cpp
        src/Systemic/DataStructures/ByteMap/ByteMap.h
        src/Systemic/DataTypes/Value/Value.cpp
        src/Systemic/DataTypes/Value/Value.h
        src/Systemic/DataTypes/DateTime/DateTime.cpp
        src/Systemic/DataTypes/DateTime/DateTime.h
        src/Systemic/DataTypes/Decimal/Decimal.cpp
        src/Systemic/DataTypes/Decimal/Decimal.h
        src/Systemic/Converter/Converter.h
        src/Systemic/DataStructures/HashSet/HashSet.h
        src/Systemic/DataTypes/SortCondition/SortCondition.cpp
        src/Systemic/DataTypes/GroupCondition/GroupCondition.cpp
        src/Systemic/DataTypes/GroupCondition/GroupCondition.h
        src/Systemic/Network/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.cpp
        src/Systemic/Network/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.h
        src/Systemic/Network/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.cpp
        src/Systemic/Network/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h
        src/Systemic/Network/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.cpp
        src/Systemic/Network/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.h
        src/Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.cpp
        src/Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h
        src/Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.cpp
        src/Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h
        src/Systemic/DataTypes/Headers/Headers.h
        src/Systemic/Functions/StringFunctions.h
        src/Systemic/DataTypes/Guid/Guid.cpp
        src/Systemic/DataTypes/Guid/Guid.h
        src/Systemic/Functions/DateFunctions.h
        src/Systemic/Coercions/Coercions.h
        src/Systemic/Coercions/Coercions.cpp
        src/Systemic/Errors/Errors.h
        src/Systemic/Indexing/Key.h
        src/Systemic/Indexing/Key.cpp
        src/Systemic/QueryResult/QueryResult.cpp
        src/Systemic/QueryResult/QueryResult.h
        src/Systemic/Network/Session.h
        src/Systemic/Security/Security.h
        src/Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.cpp
        src/Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.h
        src/Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.cpp
        src/Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h
        src/Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.cpp
        src/Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h
        src/Systemic/Network/Protocols/ConnectionProtocol/ConnectionHeader.h
        src/Systemic/DataStructures/Vector/Vector.h

)

add_library(ExpressionsLibrary STATIC
            src/Expressions/Expression.h
            src/Expressions/Expression.cpp
            src/Expressions/Expressions.Additional.h
)

target_link_libraries(ExpressionsLibrary PRIVATE Systemic PRIVATE Database)

add_library(QueryPipeline STATIC
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
)

add_dependencies(QueryPipeline GenerateSQLParser)

if(WIN32)
    target_link_libraries(QueryPipeline PRIVATE ExpressionsLibrary PRIVATE Systemic PRIVATE antlr4_shared)
else()
    target_link_libraries(QueryPipeline PRIVATE ExpressionsLibrary PRIVATE Systemic PRIVATE antlr4_static)
endif()

# Include the generated parser and lexer files
target_include_directories(QueryPipeline PRIVATE
        ${GENERATED_DIR}
)

add_library(Database STATIC
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
        src/Database/Column/IdentityManager/IdentityManager.cpp
        src/Database/Column/IdentityManager/IdentityManager.h
)

add_library(Server STATIC
        src/Server/ConnectionManager/ConnectionManager.cpp
        src/Server/ConnectionManager/ConnectionManager.h
        src/Server/Threadpool/ThreadPool.cpp
        src/Server/Threadpool/ThreadPool.h
        src/Server/Server.cpp
        src/Server/Server.h
        src/Server/MasterDbColumns.h
        src/Server/Server.Constants.h
        src/Server/SessionManager/SessionManager.cpp
        src/Server/SessionManager/SessionManager.h
        src/Server/RoleManager/RoleManager.cpp
        src/Server/RoleManager/RoleManager.h
        src/Server/UserManager/UserManager.cpp
        src/Server/UserManager/UserManager.h
)

target_link_libraries(Server PRIVATE Database PRIVATE nlohmann_json::nlohmann_json PRIVATE $<$<PLATFORM_ID:Windows>:ws2_32> PRIVATE Systemic PRIVATE unofficial::argon2::libargon2)

target_link_libraries(Database PRIVATE Systemic PRIVATE ExpressionsLibrary PRIVATE Server)

 add_executable(Client
                src/Client/client.cpp
                src/Client/client.h
         src/Client/ClientManager/ClientManager.cpp
         src/Client/ClientManager/ClientManager.h)

target_link_libraries(Client
        PRIVATE
        Systemic
        $<$<PLATFORM_ID:Windows>:ws2_32>
)

target_link_libraries(DatabaseInterface 
                        PRIVATE Server Database Systemic QueryPipeline)
