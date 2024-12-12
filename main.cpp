﻿#include <bitset>
#include <chrono>
#include <cstring>

#include "AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "./Database/Database.h"
#include "AdditionalLibraries/BitMap/BitMap.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/PageManager/PageManager.h"
#include "Database/Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h"
#include "AdditionalLibraries/AdditionalObjects/Decimal/Decimal.h"

template<typename T>
int CreateResponse(T input) { return static_cast<int>(input); }
void CreateAndInsertToDatabase(Database* db, Table* table = nullptr);
//handle updates
//deletes
//B+ Trees
//row ids
//object ids

int main()
{
    Database* db = nullptr;
    FileManager fileManager;
    PageManager* pageManager = new PageManager(&fileManager);
    const string dbName = "stakosDb";
    try
    {
        /*Get pages in select in chunks*/
        // CreateDatabase(dbName, &fileManager, pageManager);

        UseDatabase(dbName, &db, pageManager);

        pageManager->BindDatabase(db);

        Table* table = db->OpenTable("Movies");
        // CreateAndInsertToDatabase(db, table);

        table = db->OpenTable("Movies");
        constexpr int searchKey = 90;
        vector<RowCondition*> conditions;
        RowCondition condition2(&searchKey, sizeof(int), 0);

        conditions.push_back(&condition2);
        
        vector<Row> rows;

        const auto start = std::chrono::high_resolution_clock::now();
        
        table->Select(rows, &conditions);

        const auto end = std::chrono::high_resolution_clock::now();

        const auto elapsed = std::chrono::duration<double, std::milli>(end - start);

        cout << "Time elapsed : " << elapsed.count() << "ms" << endl;

        PrintRows(rows);
 
        delete db;
        delete pageManager;
    }
    catch (const exception& exception)
    {
        cerr<< exception.what() << '\n';
    }

    return 0;
}


void CreateAndInsertToDatabase(Database* db, Table* table)
{
    if(table == nullptr)
    {
        vector<Column*> columns;
        columns.push_back(new Column("MovieID", "Int", sizeof(int), false));
        columns.push_back(new Column("MovieName", "String", 100, true));
        columns.push_back(new Column("MovieType", "String", 100, true));
        columns.push_back(new Column("MovieDesc", "String", 100, true));
        columns.push_back(new Column("MovieActors", "String", 100, true));
        columns.push_back(new Column("MovieLength", "String", 100, true));

        table = db->CreateTable("Movies", columns);
    }
    
    vector<vector<Field>> inputData;
    
    for(int i = 0;i < 1000; i++)
    {
        vector<Field> fields = {
            Field("1", 0),
            Field("Silence Of The Lambs", 1),
            Field("Thriller", 2),
            Field("Du Hast Miesch", 3),
            Field("hello" + to_string(i), 4),
            Field("Hello its me you are llooooking for", 5),
        };

        fields[0].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }
    
    table->InsertRows(inputData);
}