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
        src/Systemic/src/DataTypes/Value.cpp
        src/Systemic/include/DataTypes/DataTypes.h
)

target_include_directories(Systemic PUBLIC src/Systemic/include)

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
        ${ANTLR_SRC}
)

target_include_directories(QueryPipeline PUBLIC src/QueryPipeline/include)

add_dependencies(QueryPipeline GenerateSQLParser)

if(WIN32)
    target_link_libraries(QueryPipeline PRIVATE Database PRIVATE antlr4_shared)
else()
    target_link_libraries(QueryPipeline PRIVATE Database PRIVATE antlr4_static)
endif()

# Include the generated parser and lexer files
target_include_directories(QueryPipeline PRIVATE ${GENERATED_DIR})

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
        src/Database/src/Schedulers/GarbageCollector.cpp
        src/Database/src/SystemDatabases/VersionDatabase.cpp
        src/Database/include/Schedulers/StatisticsScheduler.h
        src/Database/include/SystemDatabases/SystemCatalog.h
        src/Database/src/SystemDatabases/SystemCatalog.cpp
        src/Database/src/Evaluators/Expression.cpp
)

target_include_directories(Database PUBLIC src/Database/include)

target_link_libraries(Database PRIVATE Systemic PRIVATE nlohmann_json::nlohmann_json)

add_library(Server STATIC
        src/Server/src/ConnectionManager/ConnectionManager.cpp
        src/Server/src/Threadpool/ThreadPool.cpp
        src/Server/src/Server.cpp
        src/Server/src/SessionManager/SessionManager.cpp
        src/Server/src/RoleManager/RoleManager.cpp
        src/Server/src/UserManager/UserManager.cpp
)

target_include_directories(Server PUBLIC src/Server/include)

target_link_libraries(Server PRIVATE QueryPipeline PRIVATE $<$<PLATFORM_ID:Windows>:ws2_32> PRIVATE unofficial::argon2::libargon2)

 add_executable(Client
         src/Client/src/client.cpp
         src/Client/src/ClientManager.cpp
         src/Client/include/client.h
         src/Client/include/ClientManager.h
         src/Database/include/ExecutionProperties.h
 )

target_link_libraries(Client PRIVATE Systemic $<$<PLATFORM_ID:Windows>:ws2_32> )

target_link_libraries(DatabaseEngine PRIVATE Server)
