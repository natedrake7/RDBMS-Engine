#include <bitset>
#include <chrono>

#include "./Database/Database.h"
#include "AdditionalLibraries/BitMap/BitMap.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/PageManager/PageManager.h"

template<typename T>
int CreateResponse(T input) { return static_cast<int>(input); }
void CreateAndInsertToDatabase(Database* db, Table* table = nullptr);

int main()
{
    Database* db = nullptr;
    FileManager fileManager;
    PageManager* pageManager = new PageManager(&fileManager);
    const string dbName = "stakosDb";
    try
    {
        // /*Handle LOB pages in different extents*/
        // CreateDatabase(dbName, &fileManager, pageManager);

        UseDatabase(dbName, &db, pageManager);

        pageManager->BindDatabase(db);

        // create the table and then let it be read from the heap file
        // Table* table = db->OpenTable("Movies");


        Table* table = db->OpenTable("Movies");

        // CreateAndInsertToDatabase(db, table);

        vector<Row> rows;
        table->SelectRows(&rows);
        
        for(const auto& row : rows)
            row.PrintRow();

        //update delete rows + bitmap handle, large texts. handle nulls -> datasize 0 -> 4

        //UPDATE dbo.Movies SET isNull = 0

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
        columns.push_back(new Column("MovieID", "SmallInt", sizeof(int16_t), false));
        columns.push_back(new Column("MovieName", "String", 100, true));
        columns.push_back(new Column("MovieType", "String", 100, true));
        columns.push_back(new Column("MovieDesc", "String", 100, true));
        columns.push_back(new Column("MovieActors", "String", 100, true));
        columns.push_back(new Column("MovieLength", "String", 100, true));

        table = db->CreateTable("Movies", columns);
    }
    
    // vector<string> words = {
    //     "1"
    //     ,"Silence Of The Lambs"
    //     ,"Thriller"
    //     ,"Hello its me you are loookin for"
    //     , "Du Hast"
    //     ,"2 Hours And 15 Minutes"
    // };

    vector<vector<string*>*> inputData;

    for(int i = 1;i < 2; i++)
    {
        auto* words = new vector<string*>{
            new string("1"),
            new string("Silence Of The Lambs"),
            new string("Thriller"),
            new string("Hello its me"),
            nullptr,
            new string("2 Hours And 15 Minutes")
        };
        *(*words)[0] = to_string(i); // Dereference the pointer to modify the first string
        inputData.push_back(words);
    }

    table->InsertRows(inputData);

    for (const auto& row : inputData) {
        for (const auto& word : *row)
            delete word; // Free each string
        delete row;      // Free the vector itself
    }
}