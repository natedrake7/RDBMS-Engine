#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <string>
#include <vector>
#include "Database/Column/Column.h"
#include "Database/Constants.h"
#include "Database/Storage/StorageManager/StorageManager.h"
#include "QueryPipeline/Parser/Parser.h"
#include "Server/ConnectionManager/ConnectionManager.h"
#include "Server/Threadpool/ThreadPool.h"
#include "Server/Server.h"

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
//add further validations
//allow [] on IDENTIFIERS
//Add joins
//check index deletes work
//add identity
//add reset identity
//start documenting implementation and optimize wherever possible
//add order by statements full support (minor just add ASC, DESC on each column or on all)
//and check if index index is available in the results to speed by sorting
//Futher improve select, insert, update statements to allow nested select in them (complex validation will be required)
//allow ctes and tempporary tables.
//WAL
//tempdb
//transactions(usage of tempDb maybe).
//used aliases in logical and physical table scans and joins.
//fix aliases not working on projection
//add on columns checking to be case insensitive and all names to be normalized to lower string
//add default values on master db

std::atomic<bool> serverRunning{true};

void shutdownServer(int signal) {
    serverRunning.store(false);

    cout << "Server shutting down..." << endl;
    ServerInstance::Get().Shutdown();
    exit(0);
}

void InitializeServer(const string& filePath) {
    //create sys tables(read from file).
}

int main()
{
    signal(SIGINT, shutdownServer);   // Ctrl+C
    signal(SIGTERM, shutdownServer);  // kill command
    signal(SIGABRT, shutdownServer);  // abort()

    auto& server = ServerInstance::Get();

    server.Initialize("configuration.json");

    const int32_t databaseId = 2;

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

    // QueryPipeline::Parser::Parse(createDb, databaseId);
    // QueryPipeline::Parser::Parse(createActorsTable, databaseId);

    //  // QueryPipeline::Parser::Parse(createMoviesTable, databaseId);
    // QueryPipeline::Parser::Parse(updateActors, databaseId);
    //
    // QueryPipeline::Parser::Parse(insertMovies, databaseId);
    // QueryPipeline::Parser::Parse(selectMovies, databaseId);

//

    // QueryPipeline::Parser::Parse(createActorsTable, databaseId);
//
//       QueryPipeline::Parser::Parse(insertActors, databaseId);
// // ////
// //
      // QueryPipeline::Parser::Parse(selectActors, databaseId);


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

        cout << "Time: " << elapsed.count() << " ms" << endl;
    }

     // QueryPipeline::Parser::Parse(actorsIndex, databaseId);
    // QueryPipeline::Parser::Parse(deleteMovies, dbName);

    const auto& databases = server.GetCatalog();

    ServerInstance::Get().Shutdown();

    return 0;

    QueryPipeline::Parser::Parse(createDb, databaseId);

    QueryPipeline::Parser::Parse(createActorsTable, databaseId);

    QueryPipeline::Parser::Parse(schemaCreate, databaseId);

    QueryPipeline::Parser::Parse(createMoviesTable, databaseId);

    QueryPipeline::Parser::Parse(insertMovies, databaseId);

    QueryPipeline::Parser::Parse(insertActors, databaseId);

    QueryPipeline::Parser::Parse(selectActors, databaseId);
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
//
// void CreateActorsTable(Database *db)
// {
//     vector<Column *> columns;
//     columns.push_back(new Column("ActorId", "Int", sizeof(int32_t), false));
//     columns.push_back(new Column("ActorName", "String", 100, true));
//     columns.push_back(new Column("ActorAge", "TinyInt", sizeof(int8_t), true));
//     columns.push_back(new Column("ActorBirthDay", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
//     columns.push_back(new Column("ActorHeight", "Decimal", 10, true));
//
//     const vector<column_index_t> clusteredIndexes = { 0 };
//     const vector<vector<column_index_t>> nonClusteredIndexes = { { 1 } };
//
//     Table* table = db->CreateTable("Actors", columns, &clusteredIndexes, nullptr);
//     vector<vector<Field>> inputData;
//
//     for (int i = 0; i < 100000; i++)
//     {
//         vector<Field> fields = {
//              Field("1", 0)
//             ,Field("Johnny Depp", 1)
//             ,Field("65", 2)
//             ,Field("1962-04-12 12:12:12", 3)
//             ,Field("1.77", 4)
//         };
//
//         fields[0].SetData(to_string(i));
//
//         // table->InsertRow(fields);
//         inputData.push_back(fields);
//     }
//
//     table->InsertRows(inputData);
// }
//
// void CreateMoviesTables(Database *db)
// {
//     vector<Column *> columns;
//     columns.push_back(new Column("MovieID", "Int", sizeof(int32_t), false));
//     columns.push_back(new Column("MovieYear", "Int", sizeof(int32_t), false));
//     columns.push_back(new Column("MovieType", "UnicodeString", 100, true));
//     columns.push_back(new Column("MovieReleaseDate", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
//     columns.push_back(new Column("IsMovieLicensed", "Bool", sizeof(bool), true));
//     columns.push_back(new Column("MovieLength", "Decimal", 10, true));
//
//     const vector<column_index_t> clusteredIndexes = {0, 4};
//     const vector<vector<column_index_t>> nonClusteredIndexes = { { 1, 0 } };
//
//     Table* table = db->CreateTable("Movies", columns, &clusteredIndexes, &nonClusteredIndexes);
//
//     vector<vector<Field>> inputData;
//
//     for (int i = 0; i < 10000; i++)
//     {
//         vector<Field> fields = {
//             Field("1", 0),
//             Field("1", 1),
//             Field(u"Η Σιγή των αμνών", 2),
//             Field("2024-04-12 12:12:12", 3),
//             Field("1", 4),
//             Field("1233232.12434343", 5),
//         };
//
//         fields[0].SetData(to_string(i));
//         fields[1].SetData(to_string(i));
//
//         // table->InsertRow(fields);
//         inputData.push_back(fields);
//     }
//
//     table->InsertRows(inputData);
// }
//
// void InsertRowsToActorsTable(Table* table)
// {
//     vector<vector<Field>> inputData;
//
//     for (int i = 0; i < 100; i++)
//     {
//         vector<Field> fields = {
//              Field("1", 0)
//             ,Field("Johhny Depp", 1)
//             ,Field("65", 2)
//             ,Field("1962-04-12 12:12:12", 3)
//             ,Field("1.77", 4)
//         };
//
//         fields[0].SetData(to_string(i));
//
//         // table->InsertRow(fields);
//         inputData.push_back(fields);
//     }
//
//     table->InsertRows(inputData);
// }
//
// void InsertRowsToMoviesTable(Table* table)
// {
//     vector<vector<Field>> inputData;
//
//     for (int i = 200000; i < 200200; i++)
//     {
//         vector<Field> fields = {
//             Field("1", 0),
//             Field("1", 1),
//             Field("Thriller", 2),
//             Field("2024-04-12 12:12:12", 3),
//             Field("1", 4),
//             Field("1233232.12434343", 5),
//         };
//
//         fields[0].SetData(to_string(i));
//         fields[1].SetData(to_string(i));
//
//         // table->InsertRow(fields);
//         inputData.push_back(fields);
//     }
//
//     table->InsertRows(inputData);
// }