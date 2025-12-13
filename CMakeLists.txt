cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(Database)

set(BUILD_SHARED_LIBS ON)

add_executable(
        Database main.cpp
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
        src/Systemic/include/Constants.h
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
    target_link_libraries(QueryPipeline PRIVATE DatabaseEngine PRIVATE antlr4_shared)
else()
    target_link_libraries(QueryPipeline PRIVATE DatabaseEngine PRIVATE antlr4_static)
endif()

# Include the generated parser and lexer files
target_include_directories(QueryPipeline PRIVATE ${GENERATED_DIR})

add_library(PluginApi SHARED
        src/Plugins/src/Plugin.cpp
)

target_include_directories(PluginApi PUBLIC src/Plugins/include)

add_library(DatabaseEngine STATIC
        src/DatabaseEngine/src/Database.cpp
        src/DatabaseEngine/src/Database.IndexHandlers.cpp
        src/DatabaseEngine/src/DataStorage/Table/Table.cpp
        src/DatabaseEngine/src/DataStorage/Table/Table.AdditionalFunctions.cpp
        src/DatabaseEngine/src/DataStorage/Table/Table.Indexes.cpp
        src/DatabaseEngine/src/DataStorage/Table/Table.LargeOjects.cpp
        src/DatabaseEngine/src/DataStorage/Row.cpp
        src/DatabaseEngine/src/DataStorage/Block.cpp
        src/DatabaseEngine/src/DataStorage/Column.cpp
        src/DatabaseEngine/src/Pages/Page.cpp
        src/DatabaseEngine/src/Pages/HeaderPage.cpp
        src/DatabaseEngine/src/Pages/LargeObjectPage.cpp
        src/DatabaseEngine/src/Pages/IndexAllocationMapPage.cpp
        src/DatabaseEngine/src/Pages/PageFreeSpacePage.cpp
        src/DatabaseEngine/src/Pages/GlobalAllocationMapPage.cpp
        src/DatabaseEngine/src/Pages/IndexPage.cpp
        src/DatabaseEngine/src/Pages/OverflowPage.cpp
        src/DatabaseEngine/src/BufferPool/FileManager.cpp
        src/DatabaseEngine/src/BufferPool/StorageManager.cpp
        src/DatabaseEngine/src/BTree.cpp
        src/DatabaseEngine/src/Algorithms/Sort/SortingFunctions.cpp
        src/DatabaseEngine/src/Algorithms/AggregateFunctions.cpp
        src/DatabaseEngine/src/Algorithms/Sort/QuickSort.cpp
        src/DatabaseEngine/src/Algorithms/Sort/MergeSort.cpp
        src/DatabaseEngine/src/Managers/IdentityManager.cpp
        src/DatabaseEngine/src/Managers/LockManager.cpp
        src/DatabaseEngine/src/Managers/TransactionManager.cpp
        src/DatabaseEngine/src/Logger/Logger.cpp
        src/DatabaseEngine/src/Logger/WriteAheadLogger.cpp
        src/DatabaseEngine/src/Logger/CrashRecoveryLogger.cpp
        src/DatabaseEngine/src/Logger/Logger.Structures.cpp
        src/DatabaseEngine/src/Schedulers/GarbageCollector.cpp
        src/DatabaseEngine/src/SystemDatabases/VersionDatabase.cpp
        src/DatabaseEngine/src/SystemDatabases/SystemCatalog.cpp
        src/DatabaseEngine/src/Evaluators/Expression.cpp
        src/DatabaseEngine/src/Schedulers/StatisticsScheduler.cpp
        src/DatabaseEngine/include/Managers/StatisticsManager.h
        src/DatabaseEngine/src/Managers/StatisticsManager.cpp
)

target_include_directories(DatabaseEngine PUBLIC src/DatabaseEngine/include)

target_link_libraries(DatabaseEngine
        PRIVATE Systemic
        PRIVATE nlohmann_json::nlohmann_json
        PRIVATE PluginApi
)

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
)

target_include_directories(Client PUBLIC src/Client/include)

target_link_libraries(Client PRIVATE Systemic $<$<PLATFORM_ID:Windows>:ws2_32> )

target_link_libraries(Database PRIVATE Server)
