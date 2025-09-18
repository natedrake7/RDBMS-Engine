#include "src/Database/Table/Table.h"
#include "src/QueryPipeline/Parser/Parser.h"
#include "src/Server/Server.h"
#include "src/Server/ConnectionManager/ConnectionManager.h"


#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <vector>

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace Server;

void ExecuteQuery(Table* table, Database* db, const vector<column_index_t>& selectedColumnIndices);
void CreateMoviesTables(Database *db);
void CreateActorsTable(Database *db);
void InsertRowsToActorsTable(Table* table);
void InsertRowsToMoviesTable(Table* table);
// handle updates
// deletes
// row ids
// object ids
//truncate should deallocate the space used by the pages instead of marking it as free?
//delete should defragment pages when done and combine them on heap files.
//handle joins
//deletes
//advanced functions

//TODO
//Add Decimal full support
//Add Alter table (add drop columns)
//Alter table alter column allow with force to smaller datatype and cause truncation if need be.
//add further validations
//Add joins
//check index deletes work
//add reset identity
//start documenting implementation and optimize wherever possible
//add order by statements full support (minor just add ASC, DESC on each column or on all)
//and check if index index is available in the results to speed by sorting
//Futher improve select, insert, update statements to allow nested select in them (complex validation will be required)
//allow ctes and tempporary tables.
//create base Logger class and derive it for more specific Logging.
//WAL create logs for each method and verify validity, create recovery mechanism in Database Object
//tempdb
//transactions(usage of tempDb maybe).
//used aliases in logical and physical table scans and joins.
//fix aliases not working on projection
//add on columns checking to be case insensitive and all names to be normalized to lower string
//change select columns to return only non deleted and order by version DESC
//validation add default values and identity cannot be together (negates the point of the other)
//Parsing fix , allow default without null or not null (and directly set to not null)
//add versioning on delete and recreate with same name
//add pagination and cursors to stream batches of rows when they cant fit in memory
//add defragmentation thread (check Overflow pages and possibly Data Pages to defragment)
//validate length of columns to match max record_size from master DB on inserts and updates
//validate order by accepts expressions and evaluates them and then orders
//also add function expression evaluation for query results.
//Add coercion check functionality on statements
//FIx insert like update.
//TODO use coercion matrix instead of switch cases on datatype coercions

std::atomic<bool> serverRunning{true};

void shutdownServer(int signal) {
    serverRunning.store(false);

    cout << "Server shutting down..." << endl;
    ServerInstance::Get().Shutdown();
    exit(0);
}

void RegisterSignalHandlers()
{
    signal(SIGINT, shutdownServer);   // Ctrl+C
    signal(SIGTERM, shutdownServer);  // kill command
    signal(SIGABRT, shutdownServer);  // abort()
}

int main()
{
    RegisterSignalHandlers();

    auto& server = ServerInstance::Get();

    server.Initialize("configuration.json");

    constexpr int32_t databaseId = 2;

    //select statement
    const string selectActors = "SELECT * FROM dbo.Actors";

    const string selectMasterDb = "SELECT * FROM dbo.sys_tables";

    //select statement
    const string selectMovies = "SELECT * FROM dbo.Movies";

    //insert statement
    const string insertActors = "INSERT INTO dbo.Actors(ActorName, ActorDesc, ActorAge) VALUES('Henry Cavill', 'kalispera', 43)";

    const string insertMovies = "INSERT INTO dbo.Movies(ID, MovieName) VALUES(NEWID(), 'Batman: The Dark Knight')";

    const string deleteMovies = "DELETE FROM movies.Movies WHERE MovieName = 'Batman: The Dark Knight'";

    //create table
    const string createMoviesTable = "CREATE TABLE dbo.Movies ( "
                                        "ID GUID NOT NULL, "
                                        "MovieName VARCHAR(800) NOT NULL"
                                     ")";

    //create table
    const string createActorsTable = "CREATE TABLE Actors ( "
                                     "ID INT PRIMARY KEY IDENTITY(1, 1) ,"
                                     "ActorName VARCHAR(255) NULL, "
                                     "ActorDesc VARCHAR(MAX),"
                                     " ActorAge INT NOT NULL"
                                     ")";

    const string createDb = "CREATE DATABASE MoviesDb";

    const string schemaCreate = "CREATE SCHEMA movies";

    const string updateMovies = "UPDATE dbo.Movies SET MovieDesc = 'Batman Fights Bane' WHERE ID = 5";

    const string updateActors = "UPDATE dbo.Actors SET ActorDesc = 'Henry Cavill is hot' WHERE ID <> 20";

    const string actorsIndex = "CREATE INDEX idx_ActorsName ON dbo.Actors (ActorName)";

    const auto& databases = server.GetCatalog();

    //UPDATE dbo.Actors SET Name = CONCAT('Kalimera', 'HEllo')

    std::cout << "Please enter a query: "<< endl;

    while (true) {
        std::string input;

        std::getline(std::cin, input);

        if (input == "exit")
            break;

        const auto start = std::chrono::high_resolution_clock::now();

        QueryPipeline::Parser::Parse(input, databaseId);

        const auto end = std::chrono::high_resolution_clock::now();

        const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

        std::cout << "Time: " << elapsed.count() << " ms" << std::endl;
    }

    //SELECT * FROM dbo.Actors AS a INNER JOIN dbo.Movies AS m ON m.ActorID = a.ID

    server.Shutdown();

    return 0;

    Server::ConnectionParameters parameters("127.0.0.5", 1433, 20, 10);

    std::thread connectionThread(Server::InitializeConnectionManagerThread, std::ref(parameters), std::ref(serverRunning));

    try {
        cout << "Server Initialized correctly, type exit to shutdown" << endl;
        
        while (serverRunning) { }
    }
    catch (const exception& e) {
        cout << e.what() << endl;
    }

    serverRunning = false;
    
    connectionThread.join();
    
    return 0;
    // setlocale(LC_ALL, "");
    // try
    // {
    //     const string dbName = "stakosDb";
    //
    //      //CreateDatabase(dbName);
    //
    //
    //     string sql = "       'hello' (42 (53)); 43";
    //
    //     // vector<Token> tokens = Tokenizer::Get().TokenizeQuery(sql);
    //
    //     // const auto& query = Parser::Get().Parse(tokens);
    //
    //     // throw std::runtime_error("Not implemented yet");
    //
    //     Table* table = db->OpenTable("Movies");
    //     vector<column_index_t> selectedColumnIndices { 0, 1, 2};
    //
    //     // for(const auto& column: table->GetColumns())
    //     // {
    //     //     if(root->columns[0].columns[0] == "*")
    //     //     {
    //     //         selectedColumnIndices.push_back(column->GetColumnIndex());
    //     //         continue;
    //     //     }
    //
    //     //     for(const auto& columnName: root->columns[0].columns)
    //     //         if(column->GetColumnName() == columnName)
    //     //             selectedColumnIndices.push_back(column->GetColumnIndex());
    //     // }
    //
    //     // CreateMoviesTables(db);
    //     // CreateActorsTable(db);
    //
    //     //Table* actorsTable =  db->OpenTable("Actors");
    //     // InsertRowsToMoviesTable(table);
    //
    //     //InsertRowsToActorsTable(table);
    //
    //     const vector<Field> updates =
    //     {
    //         Field("Michael Jackson", 1)
    //     };
    //
    //     //table->Update(updates, nullptr);
    //
    //     ExecuteQuery(table, db, selectedColumnIndices);
    // }
    // catch (const exception &exception)
    // {
    //     cerr << exception.what() << '\n';
    // }
    //
    // delete db;
    // return 0;
}

// void ExecuteQuery(Table* table, Database* db, const vector<column_index_t>& selectedColumnIndices)
// {
//     //constexpr int searchKey = 90;
//     //vector<Field> conditions = 
//     //{
//     //    Field("5", 0 , Operator::OperatorNone, ConditionType::ConditionNone)
//     //};
//
//     vector<Row> rows;
//     vector<Row*> result;
//
//     const auto start = std::chrono::high_resolution_clock::now();
//
//     const vector<Field> conditions = {
//         Field("10", 0, Operator::GreaterThan, ConditionType::ConditionNone),
//     };
//
//     table->Select(rows, selectedColumnIndices, &conditions);
//
//     const auto end = std::chrono::high_resolution_clock::now();
//
//     result.reserve(rows.size());
//     for(auto& row: rows)
//             result.push_back(&row);
//
//     
//     const vector<JoinField> joinConditions = {
//         JoinField(Field("", 0, Operator::GreaterThan, ConditionType::ConditionNone), Field("", 0, Operator::GreaterThan, ConditionType::ConditionNone)),
//     };
//
//     Table* actorsTable = db->OpenTable("Actors");
//     
//     Database::JoinTables(rows, table, actorsTable, {0, 1}, joinConditions);
//
//     // Database::JoinTables(result, table, { Field("", 0, Operator::OperatorNone, ConditionType::ConditionNone) });
//     const auto elapsed = std::chrono::duration<double, std::milli>(end - start);
//
//     const auto orderStart = std::chrono::high_resolution_clock::now();
//
//     SortingFunctions::OrderBy(result, { SortCondition(0, SortType::DESCENDING, false)});
//
//     const auto orderEnd = std::chrono::high_resolution_clock::now();
//
//     const auto orderElapsed = std::chrono::duration<double, std::milli>(orderEnd - orderStart);
//
//     const auto groupByStart = std::chrono::high_resolution_clock::now();
//
//     const auto groupByResult = SortingFunctions::GroupBy(result, { GroupCondition(0, ColumnType::Int, AggregateFunction::COUNT, false, nullptr)});
//
//     const auto groupByEnd = std::chrono::high_resolution_clock::now();
//
//     const auto groupByElapsed = std::chrono::duration<double, std::milli>(groupByEnd - groupByStart);
//
//     //construct the query result here
//
//     PrintRows(rows);
//     
//     cout << "Time elapsed : " << elapsed.count() << "ms" << endl;
//     cout<< "Order By Time: "<< orderElapsed.count() << "ms" << endl;
//     cout<< "Group By Time: "<< groupByElapsed.count() << "ms" << endl;
// }