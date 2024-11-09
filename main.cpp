#include "./Database/Database.h"
#include "./Database/Row/Row.h"

template<typename T>
int CreateResponse(T input) { return static_cast<int>(input); }


int main()
{
    Database* db = nullptr;
    const string dbName = "stakosDb";
    try
    {
        // CreateDatabase(dbName);

        UseDatabase(dbName, &db);
        //
        // db->DeleteDatabase();

        vector<Column*> columns;
        columns.push_back(new Column("MovieID", "int", sizeof(int), true));
        columns.push_back(new Column("MovieName", "string", 100, true));

        Table* table = new Table("Movies", columns);
        db->CreateTable(table);

        // for(int i = 0;i < 100; i++)
        table->InsertRow();

        table->PrintTable();

        //always happens to avoid memory leaks
        delete db;
        // auto row = new Row (data, sizeof(int), sizeof(*value), dataSizes);

        // free(data);
        //
        // row->PrintRow();
    }
    catch (const exception& exception)
    {
        cerr<< exception.what() << '\n';
    }

    return 0;
}




