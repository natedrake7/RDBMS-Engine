cmake_minimum_required(VERSION 3.10)

# --- vcpkg integration (Windows/Linux/macOS) ---
# If VCPKG_ROOT is set and no toolchain was provided, use vcpkg's toolchain file.
# Note: This must run before project() on a fresh build directory to take effect.
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE AND DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "vcpkg toolchain file")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(Database)

set(BUILD_SHARED_LIBS ON)

add_executable(
        Database main.cpp
        UnitTests/src/UnitTests.cpp
)

find_package(nlohmann_json CONFIG REQUIRED)
find_package(unofficial-argon2 CONFIG REQUIRED)

add_library(Systemic SHARED
        src/Systemic/src/MultiThreading/Guards/Mutex.cpp
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
        src/Systemic/src/QueryResult.cpp
        src/Systemic/src/Coercions/Coercions.cpp
        src/Systemic/src/Network/Protocols/ConnectionProtocol.cpp
        src/Systemic/src/Network/Protocols/AuthorizeProtocol.cpp
        src/Systemic/src/Network/Protocols/ResponseProtocol.cpp
        src/Systemic/src/Network/Protocols/QueryProtocol.cpp
        src/Systemic/src/Network/Protocols/QueryResponseProtocol.cpp
        src/Systemic/src/DataTypes/Value.cpp
        src/Systemic/src/Memory/Functions.cpp
        src/Systemic/src/DataTypes/StringView.cpp
        src/Systemic/src/DataTypes/String.cpp
        src/Systemic/src/Serialization/JsonParser.cpp
        src/Systemic/src/Serialization/JsonBuilder.cpp
        src/Systemic/src/DataTypes/JsonBinary.cpp
        src/Systemic/src/Comparators.cpp
)

target_include_directories(Systemic PUBLIC src/Systemic/include)

add_library(QueryPipeline STATIC
        src/QueryPipeline/src/Optimizer.cpp
        src/QueryPipeline/src/Parser.cpp
        src/QueryPipeline/src/Cursor.cpp
        src/QueryPipeline/src/LogicalPlan.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.Joins.cpp
        src/QueryPipeline/src/PhysicalPlan/PhysicalPlan.AlterTable.cpp
        src/QueryPipeline/src/Statements.cpp
        src/QueryPipeline/src/ErrorListener.cpp
        src/QueryPipeline/src/CostEstimator.cpp
        src/QueryPipeline/src/Contexts/CompileContext.cpp
        src/QueryPipeline/src/RowCursor.cpp
        src/QueryPipeline/src/Parsing/Token.cpp
        src/QueryPipeline/src/Parsing/Lexer.cpp
        src/QueryPipeline/src/Parsing/SqlParser.cpp
        src/QueryPipeline/src/Parsing/SqlParser.Statements.cpp
)

target_include_directories(QueryPipeline PUBLIC src/QueryPipeline/include)


target_link_libraries(QueryPipeline PRIVATE CoreEngine)

add_library(PluginApi SHARED
        src/Plugins/src/Plugin.cpp
)

target_include_directories(PluginApi PUBLIC src/Plugins/include)

target_link_libraries(PluginApi
        PRIVATE Systemic
)

add_library(CoreEngine STATIC
        src/CoreEngine/src/Database.cpp
        src/CoreEngine/src/DataStorage/Table/Table.cpp
        src/CoreEngine/src/DataStorage/Table/Table.Indexes.cpp
        src/CoreEngine/src/DataStorage/Table/Table.LargeObjects.cpp
        src/CoreEngine/src/DataStorage/Row.cpp
        src/CoreEngine/src/DataStorage/Column.cpp
        src/CoreEngine/src/BufferPool/FileManager.cpp
        src/CoreEngine/src/BufferPool/StorageManager.cpp
        src/CoreEngine/src/Indexing/Key.cpp
        src/CoreEngine/src/Indexing/BTree.cpp
        src/CoreEngine/src/Algorithms/Sort/SortingFunctions.cpp
        src/CoreEngine/src/Algorithms/AggregateFunctions.cpp
        src/CoreEngine/src/Algorithms/Sort/QuickSort.cpp
        src/CoreEngine/src/Algorithms/Sort/MergeSort.cpp
        src/CoreEngine/src/Managers/IdentityManager.cpp
        src/CoreEngine/src/Managers/LockManager.cpp
        src/CoreEngine/src/Managers/TransactionManager.cpp
        src/CoreEngine/src/Logger/Logger.cpp
        src/CoreEngine/src/Logger/WriteAheadLogger.cpp
        src/CoreEngine/src/Logger/CrashRecoveryLogger.cpp
        src/CoreEngine/src/Logger/Logger.Structures.cpp
        src/CoreEngine/src/Schedulers/GarbageCollector.cpp
        src/CoreEngine/src/SystemDatabases/VersionDatabase.cpp
        src/CoreEngine/src/SystemDatabases/SystemCatalog.cpp
        src/CoreEngine/src/Evaluators/Expression.cpp
        src/CoreEngine/src/Schedulers/StatisticsScheduler.cpp
        src/CoreEngine/src/Managers/StatisticsManager.cpp
        src/CoreEngine/src/SystemDatabases/TemporaryDatabase.cpp
        src/CoreEngine/src/Logger/UndoLogger.cpp
        src/CoreEngine/src/DataStorage/Table/Table.Row.Creation.cpp
        src/CoreEngine/src/DataStorage/SerializedRow.cpp
        src/CoreEngine/src/BufferPool/BufferPoolMemoryManager.cpp
        src/CoreEngine/src/Pages/PageView.cpp
        src/CoreEngine/src/Pages/IndexPageView.cpp
        src/CoreEngine/src/Pages/GlobalAllocationPageView.cpp
        src/CoreEngine/src/Pages/PageFreeSpaceView.cpp
        src/CoreEngine/src/Pages/AllocationPageView.cpp
        src/CoreEngine/src/Pages/HeaderPageView.cpp
        src/CoreEngine/src/Pages/Additional/SlotDirectory.cpp
        src/CoreEngine/src/Pages/Additional/Frame.cpp
        src/CoreEngine/src/Pages/Additional/IndexPageStructs.cpp
        src/CoreEngine/src/Pages/LargeObjectView.cpp
        src/CoreEngine/src/Pages/OverflowPageView.cpp
        src/CoreEngine/src/Managers/GlobalMemoryManager.cpp
        src/CoreEngine/src/Contexts/ExecutionContext.cpp
        src/CoreEngine/src/Memory/Allocator.cpp
        src/CoreEngine/src/Memory/PersistentAllocator.cpp
        src/CoreEngine/src/Extensions/StringExtensions.cpp
        src/CoreEngine/src/Vectorization/Vectorization.cpp
        src/CoreEngine/src/Evaluators/Kernels/Vectorized/VectorizedKernels.cpp
        src/CoreEngine/src/DataStorage/Table/Table.Vectorized.cpp
        src/CoreEngine/src/Evaluators/Kernels/Row/RowKernels.cpp
        src/CoreEngine/src/Evaluators/Kernels/Row/RowKernels.Binary.cpp
        src/CoreEngine/src/Evaluators/Kernels/Row/RowKernels.Cast.cpp
        src/CoreEngine/src/DataStorage/ExtentReservation.cpp
        src/CoreEngine/src/Database.Templates.cpp
        src/CoreEngine/src/Evaluators/Kernels/Vectorized/Vectorized.JumpTables.cpp
)

target_include_directories(CoreEngine PUBLIC src/CoreEngine/include)

target_link_libraries(CoreEngine
        PRIVATE Systemic
        PRIVATE nlohmann_json::nlohmann_json
        PRIVATE PluginApi
)

add_library(Server STATIC
        src/Server/src/Server.cpp
        src/Server/src/Threadpool/ThreadPool.cpp
        src/Server/src/ConnectionManager/ConnectionManager.cpp
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

target_link_libraries(Client PRIVATE Systemic $<$<PLATFORM_ID:Windows>:ws2_32>)

target_link_libraries(Database PRIVATE Server)
