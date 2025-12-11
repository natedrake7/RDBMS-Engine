cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DatabaseEngine)

set(BUILD_SHARED_LIBS ON)

add_executable(
        DatabaseEngine main.cpp
        Queries.h
)

# Set this to the directory containing antlr4-runtimeConfig.cmake or antlr4-runtime-config.cmake
find_package(antlr4-runtime CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
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
        ${CMAKE_SOURCE_DIR}/src/QueryPipeline/src/Grammar/SQL.g4
        DEPENDS ${CMAKE_SOURCE_DIR}/src/QueryPipeline/src/Grammar/SQL.g4
        COMMENT "Generating ANTLR C++ parser for SQL.g4"
)

add_custom_target(
        GenerateSQLParser
        DEPENDS ${GENERATED_DIR}/SQLParser.cpp
)

file(GLOB ANTLR_SRC "${GENERATED_DIR}/*.cpp")

add_library(Systemic SHARED
        src/Systemic/src/MultiThreading/Guards/ReadWriteMutex.cpp
        src/Systemic/src/MultiThreading/Guards/ReaderGuard.cpp
        src/Systemic/src/MultiThreading/Guards/WriterGuard.cpp
        src/Systemic/src/DataStructures/BitMap.cpp
        src/Systemic/src/DataStructures/ByteMap.cpp
        src/Systemic/src/DataTypes/SortCondition.cpp
        src/Systemic/src/DataTypes/GroupCondition.cpp
        src/Systemic/src/DataTypes/Value.cpp
        src/Systemic/src/DataTypes/DateTime.cpp
        src/Systemic/src/DataTypes/Decimal.cpp
        src/Systemic/src/DataTypes/Guid.cpp
        src/Systemic/src/Key.cpp
        src/Systemic/src/QueryResult.cpp
        src/Systemic/src/Coercions.cpp
        src/Systemic/src/Network/Protocols/ConnectionProtocol.cpp
        src/Systemic/src/Network/Protocols/AuthorizeProtocol.cpp
        src/Systemic/src/Network/Protocols/ResponseProtocol.cpp
        src/Systemic/src/Network/Protocols/QueryProtocol.cpp
        src/Systemic/src/Network/Protocols/QueryResponseProtocol.cpp
        src/Systemic/include/DataStructures/BitMap.h
        src/Systemic/include/DataStructures/ByteMap.h
        src/Systemic/include/Datatypes/Value.h
        src/Systemic/include/Datatypes/DateTime.h
        src/Systemic/include/Datatypes/Decimal.h
        src/Systemic/include/Converter.h
        src/Systemic/include/DataStructures/HashSet.h
        src/Systemic/include/GroupCondition.h
        src/Systemic/include/Network/ConnectionProtocol.h
        src/Systemic/include/Network/AuthorizeProtocol.h
        src/Systemic/include/Network/ResponseProtocol.h
        src/Systemic/include/Network/QueryProtocol.h
        src/Systemic/include/Network/QueryResponseProtocol.h
        src/Systemic/include/Headers.h
        src/Systemic/include/Functions/StringFunctions.h
        src/Systemic/include/Datatypes/Guid.h
        src/Systemic/include/Functions/DateFunctions.h
        src/Systemic/include/Coercions.h
        src/Systemic/include/Errors.h
        src/Systemic/include/Key.h
        src/Systemic/include/QueryResult.h
        src/Systemic/include/Security/Session.h
        src/Systemic/include/Security/Security.h
        src/Systemic/include/Guards/ReadWriteMutex.h
        src/Systemic/include/Guards/ReaderGuard.h
        src/Systemic/include/Guards/WriterGuard.h
        src/Systemic/include/Network/ConnectionHeader.h
        src/Systemic/include/DataStructures/Vector.h
        src/Systemic/include/DataStructures/SortedDictionary.h
        src/Systemic/include/Datatypes/Variable.h
)

add_library(ExpressionsLibrary STATIC
        src/Expressions/src/Expression.cpp
        src/Expressions/include/Expression.h
        src/Expressions/include/Expressions.Additional.h
)

target_link_libraries(ExpressionsLibrary PRIVATE Systemic PRIVATE Database)

add_library(QueryPipeline STATIC
        src/QueryPipeline/src/Optimizer.cpp
        src/QueryPipeline/src/Parser.cpp
        src/QueryPipeline/src/Visitor/Visitor.cpp
        src/QueryPipeline/src/Visitor/Visitor.CreateTable.cpp
        src/QueryPipeline/src/Visitor/Visitor.Joins.cpp
        src/QueryPipeline/src/Cursor.cpp
        src/QueryPipeline/src/LogicalPlan.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.Joins.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.AlterTable.cpp
        src/QueryPipeline/src/Statements.cpp
        src/QueryPipeline/src/ErrorListener.cpp
        src/QueryPipeline/include/Parser.h
        src/QueryPipeline/include/Visitor.h
        src/QueryPipeline/include/Cursor.h
        src/QueryPipeline/include/LogicalPlan.h
        src/QueryPipeline/include/PhysicalPlan.h
        src/QueryPipeline/include/Statements.h
        src/QueryPipeline/include/ErrorListener.h
        src/QueryPipeline/include/Constants.h
        src/QueryPipeline/include/Optimizer.h
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
        src/Database/src/Database.cpp
        src/Database/src/Database.IndexHandlers.cpp
        src/Database/src/DataStorage/Table/Table.cpp
        src/Database/src/DataStorage/Table/Table.AdditionalFunctions.cpp
        src/Database/src/DataStorage/Table/Table.Indexes.cpp
        src/Database/src/DataStorage/Table/Table.LargeOjects.cpp
        src/Database/src/DataStorage/Row.cpp
        src/Database/src/DataStorage/Block.cpp
        src/Database/src/DataStorage/Column.cpp
        src/Database/src/Pages/Page.cpp
        src/Database/src/Pages/HeaderPage.cpp
        src/Database/src/Pages/LargeObjectPage.cpp
        src/Database/src/Pages/IndexAllocationMapPage.cpp
        src/Database/src/Pages/PageFreeSpacePage.cpp
        src/Database/src/Pages/GlobalAllocationMapPage.cpp
        src/Database/src/Pages/IndexPage.cpp
        src/Database/src/Pages/OverflowPage.cpp
        src/Database/src/BufferPool/FileManager.cpp
        src/Database/src/BufferPool/StorageManager.cpp
        src/Database/src/BTree.cpp
        src/Database/src/Algorithms/Sort/SortingFunctions.cpp
        src/Database/src/Algorithms/AggregateFunctions.cpp
        src/Database/src/Algorithms/Sort/QuickSort.cpp
        src/Database/src/Algorithms/Sort/MergeSort.cpp
        src/Database/src/Managers/IdentityManager.cpp
        src/Database/src/Managers/LockManager.cpp
        src/Database/src/Managers/TransactionManager.cpp
        src/Database/src/Logger/Logger.cpp
        src/Database/src/Logger/WriteAheadLogger.cpp
        src/Database/src/Logger/CrashRecoveryLogger.cpp
        src/Database/src/Logger/Logger.Structures.cpp
        src/Database/src/GarbageCollector.cpp
        src/Database/src/VersionDatabase.cpp
        src/Database/include/Database.h
        src/Database/include/DataStorage/Table.h
        src/Database/include/DataStorage/Row.h
        src/Database/include/DataStorage/Block.h
        src/Database/include/DataStorage/Column.h
        src/Database/include/Constants.h
        src/Database/include/Pages/HeaderPage.h
        src/Database/include/Pages/LargeObjectPage.h
        src/Database/include/Pages/IndexAllocationMapPage.h
        src/Database/include/Pages/PageFreeSpacePage.h
        src/Database/include/Pages/GlobalAllocationMapPage.h
        src/Database/include/Pages/IndexPage.h
        src/Database/include/Pages/Page.h
        src/Database/include/BufferPool/FileManager.h
        src/Database/include/BufferPool/StorageManager.h
        src/Database/include/BTree.h
        src/Database/include/Algorithms/Sort/SortingFunctions.h
        src/Database/include/Algorithms/AggregateFunctions.h
        src/Database/include/Algorithms/Sort/QuickSort.h
        src/Database/include/Algorithms/Sort/MergeSort.h
        src/Database/include/Managers/LockManager.h
        src/Database/include/Pages/OverflowPage.h
        src/Database/include/Logger/Logger.h
        src/Database/include/Logger/CrashRecoveryLogger.h
        src/Database/include/Logger/Logger.Structures.h
        src/Database/include/Logger/WriteAheadLogger.h
        src/Database/include/Managers/IdentityManager.h
        src/Database/include/Pages/PageGuard.h
        src/Database/include/Managers/TransactionManager.h
        src/Database/include/GarbageCollector.h
        src/Database/include/VersionDatabase.h
)

add_library(Server STATIC
        src/Server/src/ConnectionManager/ConnectionManager.cpp
        src/Server/src/Threadpool/ThreadPool.cpp
        src/Server/src/Server.cpp
        src/Server/src/SessionManager/SessionManager.cpp
        src/Server/src/RoleManager/RoleManager.cpp
        src/Server/src/UserManager/UserManager.cpp
        src/Server/include/ConnectionManager.h
        src/Server/include/ThreadPool.h
        src/Server/include/Server.h
        src/Server/include/MasterDbColumns.h
        src/Server/include/Server.Constants.h
        src/Server/include/SessionManager.h
        src/Server/include/RoleManager.h
        src/Server/include/UserManager.h
)

target_link_libraries(Server PRIVATE Database PRIVATE nlohmann_json::nlohmann_json PRIVATE $<$<PLATFORM_ID:Windows>:ws2_32> PRIVATE Systemic PRIVATE unofficial::argon2::libargon2)

target_link_libraries(Database PRIVATE Systemic PRIVATE ExpressionsLibrary PRIVATE Server)

 add_executable(Client
         src/Client/src/client.cpp
         src/Client/src/ClientManager.cpp
         src/Client/include/client.h
         src/Client/include/ClientManager.h
 )

target_link_libraries(Client PRIVATE Systemic $<$<PLATFORM_ID:Windows>:ws2_32> )

target_link_libraries(DatabaseEngine
                        PRIVATE Server Database Systemic QueryPipeline)
