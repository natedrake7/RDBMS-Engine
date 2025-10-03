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

//SELECT * FROM dbo.Actors AS A INNER JOIN dbo.Movies_RL_Actors AS MA ON A.ID = MA.ActorID

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

    //UPDATE dbo.Actors SET Name = CONCAT('Kalimera', 'HEllo')

    //
    // int value = -10000;
    // DataTypes::Decimal decimal(value);
    //
    // std::cout << decimal << std::endl;

    // DataTypes::Decimal decimal2("0");
    // //
    // std::cout << "Decimal: " << decimal + decimal2 << std::endl;

    // return 0;
    RegisterSignalHandlers();

    auto& server = ServerInstance::Get();

    server.Initialize("configuration.json");

    constexpr int32_t databaseId = 2;

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

    //CREATE TABLE dbo.NewActors(ActorID INT PRIMARY KEY, ActorName STRING(200), MovieID INT)

    const auto& databases = server.GetCatalog();

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
}