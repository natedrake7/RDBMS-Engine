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

        Table* table = new Table("Movies", columns, db);
        db->CreateTable(table);

        vector<string> words = {
            "0"
            , "Silence Of The Lambs"
            , "Thriller"
            , "A detective searches for a serial killer after conducting an experiment with Dr Hannibal Lecter and uncovers some harsh truths(that blacks do die first)"
            , "Donald J Trump"
            , "2 Hours And 15 Minutes"
        };

        const string characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        string randomString;

        // Generate random string of the given length


        auto insertStart = chrono::high_resolution_clock::now();

        for(int i = 0;i < 10000; i++) {
            words[0] = to_string(i);
            table->InsertRow(words);
        }

        auto insertEnd = chrono::high_resolution_clock::now();

        chrono::duration<double, milli> insertDuration = insertEnd - insertStart;
        cout << "Duration: " << insertDuration.count() << " ms\n";

        auto start = chrono::high_resolution_clock::now();

        char* searchTerm = "A detective searches for a serial killer after conducting an experiment with Dr Hannibal Lecter and uncovers some harsh truths(that blacks do die first)";
        size_t length = strlen(searchTerm) + 1;

        int value = 10;

        const Block searchBlock(&value, sizeof(int), columns[0]);
        vector<Column*> selectedColumns = { columns[0], columns[1] };
        const auto results = table->GetRowByBlock(searchBlock);

        table->PrintTable(10);

        auto end = chrono::high_resolution_clock::now();

        chrono::duration<double, milli> duration = end - start;
        cout << "Duration: " << duration.count() << " ms\n";

        // for(const auto& row : results)
        // {
        //     const auto& rowData = row.GetRowData();
        //
        //     for(const auto& columnData : rowData)
        //         columnData->PrintBlockData();
        // }

        // table->PrintTable();

        //always happens to avoid memory leaks
        delete db;
    }
    catch (const exception& exception)
    {
        cerr<< exception.what() << '\n';
    }

    return 0;
}




