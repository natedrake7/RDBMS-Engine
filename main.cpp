#include <chrono>

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
        columns.push_back(new Column("MovieID", "Int", sizeof(int), false));
        columns.push_back(new Column("MovieName", "String", 100, true));
        columns.push_back(new Column("MovieType", "String", 100, true));
        columns.push_back(new Column("MovieDesc", "String", 100, true));
        columns.push_back(new Column("MovieActors", "String", 100, true));
        columns.push_back(new Column("MovieLength", "String", 100, true));

        Table* table = new Table("Movies", columns);
        db->CreateTable(table);

        vector<string> words = {
            "0"
            , "Silence Of The Lambs"
            , "Thriller"
            , "A detective searches for a serial killer after conducting an experiment with Dr Hannibal Lecter and uncovers some harsh truths(that blacks do die first)"
            , "Donald J Trump"
            , "2 Hours And 15 Minutes"
        };

        auto start = chrono::high_resolution_clock::now();

        for(int i = 0;i < 1000; i++) {
            words[0] = to_string(i);
            table->InsertRow(words);
        }

        auto end = chrono::high_resolution_clock::now();

        chrono::duration<double, milli> duration = end - start;
        cout << "Duration: " << duration.count() << " ms\n";

        // table->PrintTable();

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




