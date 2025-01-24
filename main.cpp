#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include "./Database/Database.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
#include "AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "Database/Column/Column.h"
#include "Database/Constants.h"
#include "Database/AdditionalFunctions/SortingFunctions.h"
#include "Database/AggregateFunctions/AggregateFunctions.h"
#include "Database/Storage/StorageManager/StorageManager.h"
#include "Database/Table/Table.h"

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;

void ExecuteQuery(Table* table);
void CreateMoviesTables(Database *db);
void CreateActorsTable(Database *db);
void InsertRowsToActorsTable(Table* table);
void InsertRowsToMoviesTable(Table* table);
// handle updates
// deletes
// B+ Trees
// row ids
// object ids
// create composite keys (minor)
//truncate should deallocate the space used by the pages instead of marking it as free?
//delete should defragment pages when done and combine them on heap files.
//handle degree correctly

int main() 
{
    Database *db = nullptr;
    try 
    {
        const string dbName = "stakosDb";

        // CreateDatabase(dbName);

        UseDatabase(dbName, &db);

        StorageManager::Get().BindDatabase(db);

        // CreateMoviesTables(db);
        // CreateActorsTable(db);

        Table* table = db->OpenTable("Movies");
        //Table* actorsTable =  db->OpenTable("Actors");
        // InsertRowsToMoviesTable(table);

        //InsertRowsToActorsTable(table);

        const vector<Field> updates = 
        {
            Field("Michael Jackson", 1, false)
        };

        //table->Update(updates, nullptr);

        ExecuteQuery(table);
    }
    catch (const exception &exception) 
    {
        cerr << exception.what() << '\n';
    }

    delete db;
    return 0;
}

void ExecuteQuery(Table* table)
{
    //constexpr int searchKey = 90;
    //vector<Field> conditions = 
    //{
    //    Field("5", 0 , Operator::OperatorNone, ConditionType::ConditionNone)
    //};

    vector<Row> rows;
    vector<Row*> result;

    const auto start = std::chrono::high_resolution_clock::now();

    table->Select(rows);

    const auto end = std::chrono::high_resolution_clock::now();

    for(auto& row: rows)
        result.push_back(&row);

    const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

    const auto orderStart = std::chrono::high_resolution_clock::now();

    SortingFunctions::OrderBy(result, { SortCondition(0, SortType::ASCENDING, true)});

    const auto orderEnd = std::chrono::high_resolution_clock::now();

    const auto orderElapsed = std::chrono::duration<double, std::milli>(orderEnd - orderStart);

    // const long double val = 1;
    // const auto countRes = AggregateFunctions::Count(rows, 0, &val);
    
    PrintRows(result);

    // cout << fixed << setprecision(2) << "Count: " << countRes << '\n';
    
    cout << "Time elapsed : " << elapsed.count() << "ms" << endl;
    cout<< "Order By Time: "<< orderElapsed.count() << "ms" << endl;
}

void CreateActorsTable(Database *db) 
{
    vector<Column *> columns;
    columns.push_back(new Column("ActorId", "Int", sizeof(int32_t), false));
    columns.push_back(new Column("ActorName", "String", 100, true));
    columns.push_back(new Column("ActorAge", "TinyInt", sizeof(int8_t), true));
    columns.push_back(new Column("ActorBirthDay", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
    columns.push_back(new Column("ActorHeight", "Decimal", 10, true));

    const vector<column_index_t> clusteredIndexes= { 0 };
    const vector<vector<column_index_t>> nonClusteredIndexes = { { 1 } };

    Table* table = db->CreateTable("Actors", columns, &clusteredIndexes, nullptr);
    vector<vector<Field>> inputData;

    for (int i = 0; i < 100000; i++) 
    {
        vector<Field> fields = {
             Field("1", 0)
            ,Field("Johnny Depp", 1)
            ,Field("65", 2)
            ,Field("1962-04-12 12:12:12", 3)
            ,Field("1.77", 4)
        };

        fields[0].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}

void CreateMoviesTables(Database *db)
{
    vector<Column *> columns;
    columns.push_back(new Column("MovieID", "Int", sizeof(int32_t), false));
    columns.push_back(new Column("MovieYear", "Int", sizeof(int32_t), false));
    columns.push_back(new Column("MovieType", "String", 100, true));
    columns.push_back(new Column("MovieReleaseDate", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
    columns.push_back(new Column("IsMovieLicensed", "Bool", sizeof(bool), true));
    columns.push_back(new Column("MovieLength", "Decimal", 10, true));

    const vector<column_index_t> clusteredIndexes = {0};
    const vector<vector<column_index_t>> nonClusteredIndexes = { { 1 } };

    Table* table = db->CreateTable("Movies", columns, &clusteredIndexes, &nonClusteredIndexes);

    vector<vector<Field>> inputData;

    for (int i = 0; i < 100000; i++) 
    {
        vector<Field> fields = {
            Field("1", 0),        
            Field("1", 1),
            Field("Thriller", 2), 
            Field("2024-04-12 12:12:12", 3),
            Field("1", 4),        
            Field("1233232.12434343", 5),
        };

        fields[0].SetData(to_string(i));
        fields[1].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}

void InsertRowsToActorsTable(Table* table)
{
    vector<vector<Field>> inputData;

    for (int i = 0; i < 100; i++) 
    {
        vector<Field> fields = {
             Field("1", 0)
            ,Field("Johhny Depp", 1)
            ,Field("65", 2)
            ,Field("1962-04-12 12:12:12", 3)
            ,Field("1.77", 4)
        };

        fields[0].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}

void InsertRowsToMoviesTable(Table* table)
{
    vector<vector<Field>> inputData;

    for (int i = 200000; i < 200200; i++) 
    {
        vector<Field> fields = {
            Field("1", 0),        
            Field("1", 1),
            Field("Thriller", 2), 
            Field("2024-04-12 12:12:12", 3),
            Field("1", 4),        
            Field("1233232.12434343", 5),
        };

        fields[0].SetData(to_string(i));
        fields[1].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}