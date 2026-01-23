#include "../include/UnitTests.h"

#include <iostream>

#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../DatabaseEngine/include/Pages/IndexPage.h"
#include "../../DatabaseEngine/include/DataStorage/Column.h"
#include "../../DatabaseEngine/include/DataStorage/Table.h"
#include "../../DatabaseEngine/include/DataStorage/Block.h"
#include "../../DatabaseEngine/include/DataStorage/Row.h"

namespace Tests{
    void SignalHandler(const int signal){
        std::cerr << "Received signal " << signal << ", terminating tests." << std::endl;
        exit(signal);
    }

    void InitializeTester(){
        signal(SIGINT, SignalHandler);   // Ctrl+C
        signal(SIGTERM, SignalHandler);  // kill command
        signal(SIGABRT, SignalHandler);  // abort()
    }

    void RunTest(const TestFunctionPtr functionPtr){
        try{
            functionPtr();
        }
        catch (exception& e){
            std::cerr << "Received exception while executing tests: " << e.what() << std::endl;
        }
    }

    void IndexPageUpdate(){


        auto columns = std::vector<DatabaseEngine::StorageTypes::Column*>();
        columns.push_back(new DatabaseEngine::StorageTypes::Column("ID", DataType::Int, 4, 0, false));
        columns.push_back(new DatabaseEngine::StorageTypes::Column("Name", DataType::String, 200, 1, false));

        auto table = DatabaseEngine::StorageTypes::Table(0, 0, columns, nullptr);
        auto row = DatabaseEngine::StorageTypes::Row(table);

        Pages::IndexPage page(0, &table, false, {DataType::Int});
        page.SetSubKeys(1);

        auto* block = new DatabaseEngine::StorageTypes::Block(columns[0]);
        auto insertBlockRes = block->SetData(Value(1, 0));

        row.InsertColumnData(block, 0);

        auto* secondBlock = new DatabaseEngine::StorageTypes::Block(columns[1]);
        insertBlockRes = secondBlock->SetData(Value(std::string("Hello"), 0));

        row.InsertColumnData(secondBlock, 1);

        DataTypes::Indexing::Key key;
        int keyVal = 1;
        key.InsertKey(DataTypes::Indexing::Key(&keyVal, 4, DataType::Int));

        auto tuple = Pages::LeafNodeTuple(row, key);

        constexpr static auto numTuples = 65;
        for (int i = 0;i < numTuples; i++)
            page.InsertTuple(tuple);

        for (int i = 0;i < page.GetPageSize(); i++){
            auto [pageKey, pageRow] = page.GetLeafTuple(&table, i);
            std::cout << pageRow << std::endl;
        }

        int diff = 0;

        auto usedBytes = page.GetPageSize() * (tuple.row.TotalSize() + key.size + Pages::SlotDirectory::Size);

        std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.GetBytesLeft() << std::endl;

        std::cout << "Total page size: " << usedBytes + page.GetBytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;

        const std::vector updates = {
            Value(std::string("Hello my name is bigger bro"), 1)
        };
        auto* rowPtr = &tuple.row;

        auto res = rowPtr->Update(updates, diff);

        for (int i = 0;i < numTuples; i++)
            page.UpdateRow(rowPtr, i);

        std::cout << "After update:" << std::endl;
        for (int i = 0;i < page.GetPageSize(); i++){
            auto [pageKey, pageRow] = page.GetLeafTuple(&table, i);
            std::cout << pageRow << std::endl;
        }

        page.Defragment();

        std::cout << "After Defragmentation:" << std::endl;
        for (int i = 0;i < page.GetPageSize(); i++){
            auto [pageKey, pageRow] = page.GetLeafTuple(&table, i);
            std::cout << pageRow << std::endl;
        }

        usedBytes = page.GetPageSize() * (tuple.row.TotalSize() + key.size + Pages::SlotDirectory::Size);

        std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.GetBytesLeft() << std::endl;

        std::cout << "Total page size: " << usedBytes + page.GetBytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;

        for (const auto& column : columns){
            delete column;
        }
    }
}