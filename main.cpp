#include <bitset>
#include <chrono>

#include "AdditionalLibraries/AdditionalObjects/Field/Field.h"
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

        CreateAndInsertToDatabase(db, table);

        table = db->OpenTable("Movies");
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

    vector<vector<Field>> inputData;

    for(int i = 2;i < 10; i++)
    {
        vector<Field> fields = {
            Field("1"),
            Field("Silence Of The Lambs"),
            Field("Thriller"),
            Field("Du Hast Miesch"),
            Field("", true),
            Field(string(9000, 'A')),
        };

        fields[0].SetData(to_string(i));
        inputData.push_back(fields);
    }

    table->InsertRows(inputData);
}