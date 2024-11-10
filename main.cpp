﻿#include <chrono>

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

        auto insertStart = chrono::high_resolution_clock::now();

        for(int i = 0;i < 1000000; i++) {
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
        const auto results = table->GetRowByBlock(searchBlock);

        auto end = chrono::high_resolution_clock::now();

        chrono::duration<double, milli> duration = end - start;
        cout << "Duration: " << duration.count() << " ms\n";

        // for(const auto& row : results) {
        //     const auto& rowData = row->GetRowData();
        //     for(const auto& column : rowData)
        //         column->PrintBlockData();
        // }



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




