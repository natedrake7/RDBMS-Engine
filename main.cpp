#include "src/CoreEngine/include/Schedulers/GarbageCollector.h"
#include "src/CoreEngine/include/Schedulers/StatisticsScheduler.h"
#include "src/Plugins/include/Plugin.h"
#include "src/QueryPipeline/include/Parser.h"
#include "src/Server/include/Server.h"
#include "src/Server/include/ConnectionManager.h"
#include "src/Systemic/include/Functions/StringFunctions.h"
#include "main.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <vector>

#include "src/CoreEngine/include/BufferPool/BufferPoolMemoryManager.h"
#include "src/CoreEngine/include/Managers/GlobalMemoryManager.h"
#include "src/Systemic/include/Memory/Functions.h"
#include "UnitTests/include/UnitTests.h"

//TODO
//Add Decimal (division remains)
//Add Alter table (test alter drop and rename)
//Alter table alter column allow with force to smaller datatype and cause truncation if need be.
//check index deletes work
//add reset identity
//start documenting implementation and optimize wherever possible
//and check if index index is available in the results to speed by sorting
//Futher improve select, insert, update statements to allow nested select in them (complex validation will be required)
//allow ctes and tempporary tables.
//create base Logger class and derive it for more specific Logging.
//WAL create logs for each method and verify validity, create recovery mechanism in Database Object
//tempdb
//transactions(usage of tempDb maybe).
//change select columns to return only non deleted and order by version DESC
//validation add default values and identity cannot be together (negates the point of the other)
//Parsing fix , allow default without null or not null (and directly set to not null)
//add versioning on delete and recreate with same name
//add pagination and cursors to stream batches of rows when they cant fit in memory
//add defragmentation thread (check Overflow pages and possibly Data Pages to defragment)
//Add case insensitive on identifiers
//Add like comparison (add a caseInsensitive comparison expression for strings)
//Check batching, fix batching on insert with select.
//check expression size evaluations (on insert and update)
//decimal add base case of 0 and addition and multiplication by 0
//add division and implement decimal coercions and validations also add modulo and validations on insert for sizing
//update column expressions dont have indices assigned correctly and 0 is always set. (check decimal defualt insert value as well)
//fix null comparisons
//fix schema usage on tables
//debug joins
//SELECT * FROM dbo.Movies  AS M INNER JOIN dbo.Actors AS A ON M.ActorID = A.ID
//materialize lobs only when needed and cache them in value objects.
//on updates reset row cache
//check decimals assignments on joins
//add buffer pool optimization to allocate rows lazily based on if they are needed, else just store raw bytes from memory with basic metadata
//make identity BIGINT by default and downcast to int
//add GRANT REVOKE statements to admin user and update user permission based on given roles
//Lock masterdb to now allow select nor updates and access it through views only
//queryResponseProtocol returns an std:vector<QueryResult> which is internally std::vector<Value> so serialize that and delete ResponseRow (easy)

//SECURITY COMMANDS
//CREATE USER alice WITH PASSWORD 'secret';
//GRANT db_writer TO alice;

//create tempDB to store row versions and invoke it at each call
//SELECT * FROM dbo.Actors AS A INNER JOIN dbo.Movies_RL_Actors AS MA ON A.ID = MA.ActorID
//TODO add priority in pages to store system pages indefinetely and decrease second chance count
//TODO add page wrapper to handle page pin counts and locks releases etcE
//TODO add commits and rollbacks.
//Also update row version pointers on commit to point to the latest version and not have to traverse the linked list on each select
//TODO add isolation levels (read uncommitted, read committed, repeatable read, serializable)
//TODO add deadlock detection and resolution mechanism
//validate correct versionDb implementation
//Added batch streamline
//need to fix bug on background thread
//check sys_column_stats implementation.
//check why tree order is desc and not asc
//Improve index seek selection and split or statements in sub queries
//when or statments exist break into multiple index seek queries(if not index seek dont break go to index scan)
//create union logical and physical plan node to concatanate the results of the subqueries(later add it as a command)
//store or available values by operation type and by operation prededence(on each subquery) find
//the 2 or 1 values that matter(range query should have 1 min 1 max if equality exists it has precedence)

//implement clean bulk insert functionality
//improve page split factor on indexes to be more compact and void multiple pages split
//check heap insert (add locks)

//TODO improve cost estimation and statistics collection
//TODO implement better query optimization techniques (dynamic programming, genetic algorithms, simulated annealing)
//TODO implement parallel query execution and distributed databases

//verify temp db flow implementation.
//verify page implementation

//CHECK CACHE BLOCK 0
//Implement full Forward Ptr Functionality

//add peek header functionality on page rows to delay materialization
//TODO make join usable again
//maybe use an allocator even for execution nodes etc..

//use string_views on Value AsString to avoid heap allocations //or use char[size] for stack allocation
//Binary operations should allocate a new value but only then.

//TODO fix decimal multiplication and create division for it to be complete
//CREATE DATABASE MoviesDB
//USE MoviesDB
//CREATE TABLE dbo.Actors(ID INT PRIMARY KEY IDENTITY(1,1), Name STRING(200), Age INT)
//INSERT INTO dbo.Actors(Name, Age) SELECT 'Kostas', 25
//INSERT INTO dbo.Actors(Name, Age) SELECT 'Kostas', 25 FROM dbo.Actors


//fix stats manager
//figure out why at around 10 000 inserts, it fails

//check why insert fails after shutdown.
int main(){

    // Tests::InitializeTester();
    // Tests::RunTest(&Tests::DecimalTest);
    //
    // return 0;
    // External::Plugin plugin;
    // plugin.Load("plugins/PluginLibrary.dll");
    // External::Plugin::Execute("AddNumbers");
    //
    // return 0;

    const auto memoryInfo = Memory::GetOSMemoryInfo();

    static auto& globalMemoryManager = CoreEngine::GlobalMemoryManager::Get();
    globalMemoryManager.Initialize(memoryInfo);

    globalMemoryManager.Log(std::cout, Memory::MemoryLogLevel::Bytes);

    static auto& bufferPoolMemoryManager = CoreEngine::BufferPoolMemoryManager::Get();
    bufferPoolMemoryManager.Initialize(globalMemoryManager.GetBufferPoolCapacity());

    static auto& server = Network::Server::Get();


    serverRunning.store(true);

    Network::ConnectionParameters parameters("127.0.0.5", 1433, 20, 10);

    std::thread connectionThread(Network::InitializeConnectionManagerThread, std::ref(parameters), std::ref(serverRunning));

    std::thread garbageCollectorThread(CoreEngine::GarbageCollector::Collect, std::ref(serverRunning));

    std::thread statisticsThread(
        CoreEngine::StatisticsScheduler::Start,
        std::ref(serverRunning),
        std::ref(server.GetDatabases()),
        std::ref(server.GetDatabasesLatch())
    );

    static constexpr DataTypes::StringView CONFIG_FILE_PATH = "configuration.json";
    server.Initialize(CONFIG_FILE_PATH);

    const auto* user = server.Authenticate("admin", "admin");

    if (user == nullptr) {
        server.Shutdown();
        return 0;
    }

    const auto* session = server.CreateSession(user);

    std::cout << "Please enter a query: "<< endl;

    const std::string exit = "exit";

    while (true) {
        std::string input;

        std::getline(std::cin, input);

        if (Functions::String::EqualsIgnoreCase(input, exit))
            break;

        ExecuteQuery(input, session->sessionId);
    }

    globalMemoryManager.Log(std::cout, ::Memory::MemoryLogLevel::KiloBytes);
    serverRunning.store(false, std::memory_order_relaxed);

    connectionThread.join();
    statisticsThread.join();
    garbageCollectorThread.join();

    server.Shutdown();
    return 0;
}

void ExecuteQuery(const std::string& query, const DataTypes::Guid& sessionId) {
    const auto start = std::chrono::high_resolution_clock::now();

    const auto parseResult = QueryPipeline::Parser::StartTransaction(query, sessionId);

    if (parseResult.status.hasError) {
        std::cerr << "Error: " << parseResult.status.message << std::endl;
        return;
    }

    bool hasError = false;

    auto count = 0;
    for (auto* cursor : parseResult.cursors) {
        while (cursor->CanFetch()) {
            auto batch = cursor->FetchNextBatch();
            if (!batch.status.IsOk()) {
                std::cerr << "Error: " << batch.status.message << std::endl;
                hasError = true;
                break;
            }

            count += batch.rows.Size();
            for (const auto& column : batch.displayColumnNames)
                std::cout << column << " || ";

            std::cout << std::endl;
            for (const auto& row : batch.results)
                std::cout << row;
        }

        if (hasError) {
            QueryPipeline::Parser::RollbackTransaction(sessionId, cursor);
            continue;
        }

        CoreEngine::GlobalMemoryManager::Get().Log(std::cout, ::Memory::MemoryLogLevel::KiloBytes);
        QueryPipeline::Parser::CommitTransaction(sessionId, cursor);
    }

    const auto end = std::chrono::high_resolution_clock::now();

    const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

    std::cout << "Rows affected: " << count << std::endl;

    std::cout << "Time: " << elapsed.count() << " ms" << std::endl;
}

void shutdownClient(int signal) {
    serverRunning.store(false, std::memory_order_relaxed);

    cout << "Server shutting down..." << endl;
    Network::Server::Get().Shutdown();
    exit(0);
}

void RegisterSignalHandlers(){
    signal(SIGINT, shutdownClient);   // Ctrl+C
    signal(SIGTERM, shutdownClient);  // kill command
    signal(SIGABRT, shutdownClient);  // abort()
}

void Testing(){

}