#include <chrono>

#include "./Database/Database.h"
#include "./Database/Row/Row.h"
#include "AdditionalLibraries/PageManager/PageManager.h"

template<typename T>
int CreateResponse(T input) { return static_cast<int>(input); }
void CreateAndInsertToDatabase(Database* db);

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

        // CreateAndInsertToDatabase(db);
        //
        // string largeString(9000, 'A');

        Table* table = db->OpenTable("Movies");

        // vector<string> words = {
        //     "1"
        //     , "Silence Of The Lambs"
        //     , "Thriller"
        //     ,"gdgdfgfdg"
        //     , "Donald J Trump"
        //     , "2 Hours And 15 Minutes"
        // };
        //
        // words[3] = largeString;
        //
        // for(int i = 0;i < 1; i++) {
        //     words[0] = to_string(i);
        //     table->InsertRow(words);
        // }

        const auto& rows = table->SelectRows();

        for(const auto& row : rows)
            row.PrintRow();

        //update delete rows + bitmap handle, large texts. handle nulls -> datasize 0 -> 4

        //UPDATE dbo.Movies SET isNull = 0

        // const auto& rows = table->SelectRows();
        //
        //
        // for(const auto& row : rows)
        //     row.PrintRow();

        delete db;
        delete pageManager;
    }
    catch (const exception& exception)
    {
        cerr<< exception.what() << '\n';
    }

    return 0;
}


void CreateAndInsertToDatabase(Database* db)
{
    vector<Column*> columns;
    columns.push_back(new Column("MovieID", "SmallInt", sizeof(int16_t), false));
    columns.push_back(new Column("MovieName", "String", 100, true));
    columns.push_back(new Column("MovieType", "String", 100, true));
    columns.push_back(new Column("MovieDesc", "String", 100, true));
    columns.push_back(new Column("MovieActors", "String", 100, true));
    columns.push_back(new Column("MovieLength", "String", 100, true));

    Table* table = db->CreateTable("Movies", columns);

    vector<vector<string>> inputData;
    vector<string> words = {
        "1"
        ,"Silence Of The Lambs"
        ,"Thriller"
        ,"Du Hast"
        , {}
        ,"2 Hours And 15 Minutes"
    };

    for(int i = 0;i < 100; i++)
    {
        words[0] = to_string(i);
        inputData.push_back(words);
    }

    table->InsertRows(inputData);
}