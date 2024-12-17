#include <bitset>
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
#include "AdditionalLibraries/B+Tree/BPlusTree.h"

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
        
        table->Select(rows);

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
        columns.push_back(new Column("MovieReleaseDate", "DateTime", DateTime::DateTimeSize(), true));
        columns.push_back(new Column("IsMovieLicensed", "Bool", sizeof(bool), true));
        columns.push_back(new Column("MovieLength", "Decimal", sizeof(double), true));

        table = db->CreateTable("Movies", columns);
    }
    
    vector<vector<Field>> inputData;
    
    for(int i = 0;i < 1; i++)
    {
        vector<Field> fields = {
            Field("1", 0),
            Field("Silence Of The Lambs", 1),
            Field("Thriller", 2),
            Field("2024-04-12 12:12:12", 3),
            Field("1", 4),
            Field("1233232.12434343", 5),
        };

        fields[0].SetData(to_string(i));

        // table->InsertRow(fields);
        inputData.push_back(fields);
    }
    
    table->InsertRows(inputData);
}