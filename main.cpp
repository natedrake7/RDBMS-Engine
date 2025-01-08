#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include "./Database/Database.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h"
#include "AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "Database/Column/Column.h"
#include "Database/Constants.h"
#include "Database/Storage/StorageManager/StorageManager.h"
#include "Database/Table/Table.h"

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;

void ExecuteQuery(Table* table);
void CreateMoviesTables(Database *db);
void CreateActorsTable(Database *db);
// handle updates
// deletes
// B+ Trees
// row ids
// object ids

int main() 
{
    Database *db = nullptr;
    const string dbName = "stakosDb";

    try 
    {
     //   CreateDatabase(dbName);

        UseDatabase(dbName, &db);

        StorageManager::Get().BindDatabase(db);

        Table *table = nullptr;

      //  CreateMoviesTables(db);
        //CreateActorsTable(db);

        table = db->OpenTable("Actors");

        const vector<Field> updates = {
            Field("Michael Jackson", 1, false)
        };

       // table->Update(updates, nullptr);

        table = db->OpenTable("Actors");

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

    const auto start = std::chrono::high_resolution_clock::now();

    table->Select(rows);

    const auto end = std::chrono::high_resolution_clock::now();

    const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

    PrintRows(rows);

    cout << "Time elapsed : " << elapsed.count() << "ms" << endl;
}

void CreateActorsTable(Database *db) 
{
    vector<Column *> columns;
    columns.push_back(new Column("ActorId", "Int", sizeof(int32_t), false));
    columns.push_back(new Column("ActorName", "String", 100, true));
    columns.push_back(new Column("ActorAge", "TinyInt", sizeof(int8_t), true));
    columns.push_back(new Column("ActorBirthDay", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
    columns.push_back(new Column("ActorHeight", "Decimal", 10, true));

    const vector<column_index_t> clusteredIndexes; /*= {0};*/

    Table* table = db->CreateTable("Actors", columns, &clusteredIndexes, nullptr);
    vector<vector<Field>> inputData;

    for (int i = 0; i < 100; i++) 
    {
        vector<Field> fields = {
             Field("1", 0)
            ,Field("Johhny Depp", 1)
            ,Field("65", 2)
            ,Field("1962-04-12 12:12:12", 3)
            ,Field("1.73", 4)
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
    columns.push_back(new Column("MovieName", "String", 100, true));
    columns.push_back(new Column("MovieType", "String", 100, true));
    columns.push_back(new Column("MovieReleaseDate", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
    columns.push_back(new Column("IsMovieLicensed", "Bool", sizeof(bool), true));
    columns.push_back(new Column("MovieLength", "Decimal", 10, true));

    const vector<column_index_t> clusteredIndexes = {0};

    Table* table = db->CreateTable("Movies", columns, &clusteredIndexes, nullptr);

    vector<vector<Field>> inputData;

    for (int i = 0; i < 1000; i++) 
    {
        vector<Field> fields = {
            Field("1", 0),        Field("Silence Of The Lambs" + to_string(i), 1),
            Field("Thriller", 2), Field("2024-04-12 12:12:12", 3),
            Field("1", 4),        Field("1233232.12434343", 5),
        };

        fields[0].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}