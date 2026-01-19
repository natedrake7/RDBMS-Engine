#include "src/DatabaseEngine/include/Schedulers/GarbageCollector.h"
#include "src/DatabaseEngine/include/Schedulers/StatisticsScheduler.h"
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

//VERFIY Page write to disk and read implementation

int main()
{
    //Get table stats
    //SELECT TOP(1) TS.table_id AS ID, T.name AS Name, TS.row_count AS RowCount, TS.avg_record_size AS RowSize FROM masterDb.dbo.sys_table_stats AS TS INNER JOIN masterDb.dbo.sys_tables AS T ON T.table_id = TS.table_id AS TS ORDER BY ID DESC

    // auto page = Pages::IndexPage(0, true, {DataType::Int});
    //
    // page.SetNumberOfKeys(1);
    //
    // auto column = DatabaseEngine::StorageTypes::Column("", DataType::Int, 4, 0, false);
    // auto strColumn = DatabaseEngine::StorageTypes::Column("", DataType::String, 40, 0, false);
    // auto dateColumn = DatabaseEngine::StorageTypes::Column("", DataType::DateTime, 40, 0, false);
    //
    // const std::vector columns = { &column, &strColumn, &dateColumn };
    // auto table = DatabaseEngine::StorageTypes::Table(0 , 0, columns, nullptr);
    //
    // auto key = DataTypes::Indexing::Key();
    // key.InsertKey(DataTypes::Indexing::Key(Value(10)));
    //
    // auto* row = new DatabaseEngine::StorageTypes::Row(table);
    //
    // auto* block = new DatabaseEngine::StorageTypes::Block(&column);
    // auto res = block->SetData(Value(10));
    //
    // auto* strBlock = new DatabaseEngine::StorageTypes::Block(&strColumn);
    // res = strBlock->SetData(Value(std::string("Hello World")));
    //
    // auto* dateBlock = new DatabaseEngine::StorageTypes::Block(&dateColumn);
    //
    // DataTypes::DateTime date;
    // res = dateBlock->SetData(Value(date));
    //
    // std::cout << date << std::endl;
    //
    // row->InsertColumnData(block, 0);
    // row->InsertColumnData(strBlock, 1);
    // row->InsertColumnData(dateBlock, 2);
    //
    // page.InsertChild(1, nullptr, 0);
    // page.InsertChild(2, &key, 1);
    //
    // const auto firstPage = page.GetChild(0);
    // const auto secondPage = page.GetChild(1);
    //
    // // auto newKey = page.GetKey(0);
    // // std::cout << newKey << std::endl;
    //
    // // auto page_row = page.GetRow(&table, 0);
    // // std::cout << page_row << std::endl;
    // //
    // // page_row = page.GetRow(&table, 1);
    // // std::cout << page_row << std::endl;
    // //
    // // page_row = page.GetRow(&table, 2);
    // // std::cout << page_row << std::endl;
    //
    // return 0;
    // RegisterSignalHandlers();

    static auto& server = Network::Server::Get();

    // External::Plugin plugin;
    // plugin.Load("plugins/PluginLibrary.dll");
    // External::Plugin::Execute("AddNumbers");
    //
    // return 0;

    Network::ConnectionParameters parameters("127.0.0.5", 1433, 20, 10);

    std::thread connectionThread(Network::InitializeConnectionManagerThread, std::ref(parameters), std::ref(serverRunning));

    std::thread garbageCollectorThread(DatabaseEngine::GarbageCollector::Collect, std::ref(serverRunning));

    std::thread statisticsThread(
        DatabaseEngine::StatisticsScheduler::Start,
        std::ref(serverRunning),
        std::ref(server.GetDatabases()),
        std::ref(server.GetDatabasesLatch())
    );

    server.Initialize("configuration.json");

    serverRunning.store(true);

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

    // const auto& databases = server.GetCatalog();

    serverRunning.store(false, std::memory_order_relaxed);


    connectionThread.join();
    statisticsThread.join();
    garbageCollectorThread.join();

    server.Shutdown();
    return 0;
}

void ExecuteQuery(const std::string& query, const DataTypes::Guid& sessionId) {
    const auto start = std::chrono::high_resolution_clock::now();

    const auto parserResult = QueryPipeline::Parser::StartTransaction(query, sessionId);

    if (parserResult.status.hasError) {
        std::cerr << "Error: " << parserResult.status.message << std::endl;
        return;
    }

    bool hasError = false;

    auto count = 0;
    for (auto* cursor : parserResult.cursors) {
        while (cursor->canFetch()) {
            auto batchResult = QueryPipeline::Parser::Execute(cursor);

            if (batchResult.status.hasError) {
                std::cerr << "Error: " << batchResult.status.message << std::endl;
                hasError = true;
                break;
            }

            count += static_cast<int>(batchResult.rows.size());
            for (const auto& column : batchResult.columns)
                std::cout << column << " || ";

            std::cout << std::endl;

            for (const auto& row: batchResult.rows)
                std::cout << row;
        }

        if (hasError) {
            QueryPipeline::Parser::RollbackTransaction(sessionId, cursor);
            continue;
        }

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