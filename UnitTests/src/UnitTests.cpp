#include "../include/UnitTests.h"

#include <iostream>
#include "../../CoreEngine/include/DataStorage/Table.h"
#ifdef __linux__
    #include <csignal>
#endif

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
        catch (std::exception& e){
            std::cerr << "Received exception while executing tests: " << e.what() << std::endl;
        }
    }

    void DecimalTest(){
        constexpr auto lhs = DataTypes::Decimal(100);
        constexpr auto rhs = DataTypes::Decimal(100);
        constexpr auto sum = lhs + rhs;
        std::cout << sum << std::endl;

        constexpr auto strValue = DataTypes::StringView("1000.020");
        constexpr auto strValue2 = DataTypes::StringView("-1000.020");
        constexpr auto lhs2 = DataTypes::Decimal(100);
        constexpr auto rhs2 = DataTypes::Decimal(strValue);
        constexpr auto sum2 = lhs2 + rhs2;

        std::cout << sum2 << std::endl;

        constexpr auto lhs3 = DataTypes::Decimal(strValue);
        constexpr auto rhs3 = DataTypes::Decimal(100);
        constexpr auto sum3 = lhs3 + rhs3;
        std::cout << sum3 << std::endl;

        constexpr auto lhs4 = DataTypes::Decimal(strValue);
        constexpr auto rhs4 = DataTypes::Decimal(strValue);
        constexpr auto sum4 = lhs4 + rhs4;
        std::cout << sum4 << std::endl;

        constexpr auto lhs5 = DataTypes::Decimal(strValue);
        constexpr auto rhs5 = DataTypes::Decimal(strValue);
        constexpr auto sum5 = lhs5 - rhs5;
        std::cout << sum5 << std::endl;

        constexpr auto lhs6 = DataTypes::Decimal(strValue);
        constexpr auto rhs6 = DataTypes::Decimal(strValue);
        const auto sum6 = lhs6 * rhs6;
        std::cout << sum6 << std::endl;

        constexpr auto lhs7 = DataTypes::Decimal(strValue);
        constexpr auto rhs7 = DataTypes::Decimal(strValue2);
        constexpr auto sum7 = lhs7 - rhs7;

        constexpr auto strSum7 = sum7.ToBufferString();
        std::cout << sum7 << std::endl;
    }

    void IndexPageUpdate(){
        // auto columns = std::vector<DatabaseEngine::StorageTypes::Column*>();
        // columns.push_back(new DatabaseEngine::StorageTypes::Column("ID", DataType::Int, 4, 0, false));
        // columns.push_back(new DatabaseEngine::StorageTypes::Column("Name", DataType::String, 200, 1, false));
        //
        // auto table = DatabaseEngine::StorageTypes::Table(0, 0, columns, nullptr);
        // Pages::IndexPageView page(0, &table, false, {DataType::Int});
        // page.SetSubKeys(1);
        //
        // Errors::RuntimeStatus status;
        // std::vector insertValues = {
        //     Value(1, 0),
        //     Value(std::string("Hello"), 1)
        // };
        // auto payload = table.CreateInsertPayload(status, 0, insertValues);
        //
        // DataTypes::Indexing::Key key;
        // int keyVal = 1;
        // key.InsertKey(DataTypes::Indexing::Key(&keyVal, 4, DataType::Int));
        //
        // auto tuple = Pages::IndexInsertTuple(key, &payload);
        //
        // auto insertKey = table.CreateKey({0, 1}, *tuple.payload);
        //
        // std::cout   << "Key size: " << insertKey.size << std::endl
        //             << "key: " << insertKey << std::endl;
        //
        //
        // constexpr static auto numTuples = 40;
        // for (int i = 0;i < numTuples; i++)
        //     page.InsertTuple(tuple);
        //
        //
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto [pageKey, rowPtr] = page.PeekLeafTuple(i);
        //     std::cout << rowPtr.indexPosition << std::endl;
        //
        //     auto row = rowPtr.Materialize();
        //
        //     std::cout << row << std::endl;
        //
        //     rowPtr.keySize = tuple.key.size;
        //
        //     std::cout << rowPtr.PartialMaterialize(0) << std::endl;
        //     std::cout << rowPtr.PartialMaterialize(1) << std::endl;
        // }
        //
        // int diff = 0;
        //
        // auto usedBytes = page.GetPageSize() * (tuple.payload->Size() + key.size + Pages::SlotDirectory::Size);
        //
        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;
        //
        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;
        //
        // std::vector updates = {
        //     Value(std::string("Hello my name is bigger bro"), 1)
        // };
        //
        // auto [pageKey, rowPtr] = page.PeekLeafTuple(0);
        //
        // auto row = rowPtr.Materialize();
        //
        // row.Update(updates);
        //
        // std::cout << "After update: " << row << std::endl;
        //
        // auto newPayload = table.CreateInsertPayload(status, 0, row.Data());
        // tuple.payload = &newPayload;
        //
        // page.UpdateRow(*tuple.payload, rowPtr);
        //
        // std::cout << "After update:" << std::endl;
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto [_, pagePtr] = page.PeekLeafTuple(i);
        //
        //     auto updatedRow = pagePtr.Materialize();
        //
        //     std::cout << updatedRow << std::endl;
        // }
        //
        // page.Defragment();
        //
        // std::cout << "After Defragmentation:" << std::endl;
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto [_, pagePtr] = page.PeekLeafTuple(i);
        //     auto updatedRow = pagePtr.Materialize();
        //
        //     std::cout << updatedRow << std::endl;
        // }
        //
        // usedBytes = page.GetPageSize() * (tuple.payload->Size() + key.size + Pages::SlotDirectory::Size);
        //
        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;
        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;
    }

    void PageUpdate(){
        // auto columns = std::vector<DatabaseEngine::StorageTypes::Column*>();
        // columns.push_back(new DatabaseEngine::StorageTypes::Column("ID", DataType::Int, 4, 0, false));
        // columns.push_back(new DatabaseEngine::StorageTypes::Column("Name", DataType::String, 200, 1, false));
        //
        // auto table = DatabaseEngine::StorageTypes::Table(0, 0, columns, nullptr);
        // Pages::Page page(0, &table, false);
        //
        // Errors::RuntimeStatus status;
        // std::vector insertValues = {
        //     Value(1, 0),
        //     Value(std::string("Hello"), 1)
        // };
        // auto payload = table.CreateInsertPayload(status, 0, insertValues);
        //
        // constexpr static auto numTuples = 40;
        // for (int i = 0;i < numTuples; i++)
        //     page.InsertRow(payload);
        //
        //
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto rowPtr = page.PeekRow(i, 0);
        //     std::cout << rowPtr.indexPosition << std::endl;
        //
        //     auto row = rowPtr.Materialize();
        //
        //     std::cout << row << std::endl;
        //
        //     std::cout << rowPtr.PartialMaterialize(0) << std::endl;
        //     std::cout << rowPtr.PartialMaterialize(1) << std::endl;
        // }
        //
        // int diff = 0;
        //
        // auto usedBytes = page.GetPageSize() * (payload.Size() + Pages::SlotDirectory::Size);
        //
        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;
        //
        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;
        //
        // std::vector updates = {
        //     Value(std::string("Hello my name is bigger bro"), 1)
        // };
        //
        // auto rowPtr = page.PeekRow(0, 0);
        //
        // auto row = rowPtr.Materialize();
        //
        // row.Update(updates);
        //
        // std::cout << "After update: " << row << std::endl;
        //
        // payload = table.CreateInsertPayload(status, 0, row.Data());
        //
        // page.UpdateRow(payload, rowPtr);
        //
        // std::cout << "After update:" << std::endl;
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto pagePtr = page.PeekRow(i, 0);
        //
        //     auto updatedRow = pagePtr.Materialize();
        //
        //     std::cout << updatedRow << std::endl;
        // }
        //
        // page.Defragment();
        //
        // std::cout << "After Defragmentation:" << std::endl;
        // for (int i = 0;i < page.GetPageSize(); i++){
        //     auto pagePtr = page.PeekRow(i, 0);
        //
        //     auto updatedRow = pagePtr.Materialize();
        //
        //     std::cout << updatedRow << std::endl;
        // }
        //
        // usedBytes = page.GetPageSize() * (payload.Size() + Pages::SlotDirectory::Size);
        //
        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;
        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << INDEX_PAGE_DEFAULT_SIZE << std::endl;
    }
}
