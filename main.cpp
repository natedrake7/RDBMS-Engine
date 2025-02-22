﻿#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <vector>
#include "./Database/Database.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
#include "AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "AdditionalLibraries/AdditionalDataTypes/GroupCondition/GroupCondition.h"
#include "Database/Column/Column.h"
#include "Database/Constants.h"
#include "Database/AdditionalFunctions/SortingFunctions.h"
#include "Database/Storage/StorageManager/StorageManager.h"
#include "Database/Table/Table.h"
#include "QueryParser/Tokenizer/Tokenizer.h"
#include "QueryParser/AstTree/AstTree.h"

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace QueryParser;

void ExecuteQuery(Table* table, const vector<column_index_t>& selectedColumnIndices);
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
int main() 
{

    setlocale(LC_ALL, "");
    Database *db = nullptr;
    try 
    {
        const string dbName = "stakosDb";

        // CreateDatabase(dbName);

        UseDatabase(dbName, &db);

        StorageManager::Get().BindDatabase(db);

        string sql = "SELECT * FROM Movies WHERE MovieID = 25 GROUP BY MovieID HAVING MovieID > 0 ORDER BY MovieID DESC;";

        vector<QueryParser::Token> tokens = QueryParser::TokenizeQuery(sql);
        const auto& root = QueryParser::AstTree::Get().BuildTree(tokens);

        Table* table = db->OpenTable(root->table);
        vector<column_index_t> selectedColumnIndices;

        for(const auto& column: table->GetColumns())
        {
            if(root->columns[0] == "*")
            {
                selectedColumnIndices.push_back(column->GetColumnIndex());
                continue;
            }

            for(const auto& columnName: root->columns)
                if(column->GetColumnName() == columnName)
                    selectedColumnIndices.push_back(column->GetColumnIndex());
        }

        // CreateMoviesTables(db);
        // CreateActorsTable(db);

        //Table* actorsTable =  db->OpenTable("Actors");
        // InsertRowsToMoviesTable(table);

        //InsertRowsToActorsTable(table);

        const vector<Field> updates = 
        {
            Field("Michael Jackson", 1, false)
        };

        //table->Update(updates, nullptr);

        ExecuteQuery(table, selectedColumnIndices);
    }
    catch (const exception &exception) 
    {
        cerr << exception.what() << '\n';
    }

    delete db;
    return 0;
}

void ExecuteQuery(Table* table, const vector<column_index_t>& selectedColumnIndices)
{
    //constexpr int searchKey = 90;
    //vector<Field> conditions = 
    //{
    //    Field("5", 0 , Operator::OperatorNone, ConditionType::ConditionNone)
    //};

    vector<Row> rows;
    vector<Row*> result;

    const auto start = std::chrono::high_resolution_clock::now();

    const vector<Field> conditions = {
        Field("10", 0, Operator::GreaterThan, ConditionType::ConditionNone),
    };

    table->Select(rows, selectedColumnIndices, &conditions);

    const auto end = std::chrono::high_resolution_clock::now();

    result.reserve(rows.size());
    for(auto& row: rows)
            result.push_back(&row);

    // Database::JoinTables(result, table, { Field("", 0, Operator::OperatorNone, ConditionType::ConditionNone) });

    const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

    const auto orderStart = std::chrono::high_resolution_clock::now();

    SortingFunctions::OrderBy(result, { SortCondition(0, SortType::DESCENDING, false)});

    const auto orderEnd = std::chrono::high_resolution_clock::now();

    const auto orderElapsed = std::chrono::duration<double, std::milli>(orderEnd - orderStart);

    const auto groupByStart = std::chrono::high_resolution_clock::now();

    const auto groupByResult = SortingFunctions::GroupBy(result, { GroupCondition(0, ColumnType::Int, AggregateFunction::COUNT, false, nullptr)});

    const auto groupByEnd = std::chrono::high_resolution_clock::now();

    const auto groupByElapsed = std::chrono::duration<double, std::milli>(groupByEnd - groupByStart);

    //construct the query result here

    PrintRows(result);
    
    cout << "Time elapsed : " << elapsed.count() << "ms" << endl;
    cout<< "Order By Time: "<< orderElapsed.count() << "ms" << endl;
    cout<< "Group By Time: "<< groupByElapsed.count() << "ms" << endl;
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
    columns.push_back(new Column("MovieType", "UnicodeString", 100, true));
    columns.push_back(new Column("MovieReleaseDate", "DateTime", DataTypes::DateTime::DateTimeSize(), true));
    columns.push_back(new Column("IsMovieLicensed", "Bool", sizeof(bool), true));
    columns.push_back(new Column("MovieLength", "Decimal", 10, true));

    const vector<column_index_t> clusteredIndexes = {0, 4};
    const vector<vector<column_index_t>> nonClusteredIndexes = { { 1, 0 } };

    Table* table = db->CreateTable("Movies", columns, &clusteredIndexes, &nonClusteredIndexes);

    vector<vector<Field>> inputData;

    for (int i = 0; i < 10000; i++) 
    {
        vector<Field> fields = {
            Field("1", 0),        
            Field("1", 1),
            Field(u"Η Σιγή των αμνών", 2), 
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