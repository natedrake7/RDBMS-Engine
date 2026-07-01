#include "../include/UnitTests.h"

#include <csignal>
#include <iostream>
#include "../../CoreEngine/include/DataStorage/Table.h"
#include "../../CoreEngine/include/DataStorage/Column.h"
#include "../../CoreEngine/include/Pages/Additional/Frame.h"
#include "../../src/Systemic/include/Serialization/JsonParser.h"
#include "../../src/Systemic/include/Serialization/JsonBuilder.h"
#include "../../src/Systemic/include/DataTypes/JsonBinary.h"

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

        constexpr auto strValue = DataTypes::StringView("1000.23");
        constexpr auto strValue2 = DataTypes::StringView("1000");
        constexpr auto lhs2 = DataTypes::Decimal(100020);
        constexpr auto rhs2 = DataTypes::Decimal(strValue);
        constexpr auto sum2 = lhs2 + rhs2;

        constexpr auto result = lhs2 == rhs2;

        constexpr auto strSum2 = sum2.ToBufferString();

        constexpr Int roundSum2 = sum2.ToInt<Int>();

        std::cout << sum2 << std::endl;

        constexpr auto lhs3 = DataTypes::Decimal(strValue);
        constexpr auto rhs3 = DataTypes::Decimal(100);
        constexpr auto sum3 = lhs3 + rhs3;
        std::cout << sum3 << std::endl;

        constexpr auto lhs4 = DataTypes::Decimal(strValue);
        constexpr auto rhs4 = DataTypes::Decimal(strValue);
        constexpr auto sum4 = lhs4 + rhs4;
        constexpr auto strSum4 = sum4.ToBufferString();
        std::cout << sum4 << std::endl;

        constexpr auto lhs5 = DataTypes::Decimal(strValue);
        constexpr auto rhs5 = DataTypes::Decimal(strValue);
        constexpr auto sum5 = lhs5 - rhs5;
        std::cout << sum5 << std::endl;

        constexpr auto lhs6 = DataTypes::Decimal(strValue);
        constexpr auto rhs6 = DataTypes::Decimal(strValue);
        constexpr auto sum6 = lhs6 * rhs6;
        std::cout << sum6 << std::endl;

        constexpr auto lhs7 = DataTypes::Decimal(strValue);
        constexpr auto rhs7 = DataTypes::Decimal(strValue2);
        constexpr auto sum7 = lhs7 - rhs7;

        constexpr auto strSum7 = sum7.ToBufferString();
        std::cout << strSum7.Data() << std::endl;
    }

    void IndexPageUpdate(){
        CoreEngine::Memory::Allocator allocator;

        auto column1 = allocator.Allocate<CoreEngine::StorageTypes::Column>("ID", DataType::Int, 4, 0, false);
        auto column2 = allocator.Allocate<CoreEngine::StorageTypes::Column>("Name", DataType::String, 200, 1, false);

        auto table = CoreEngine::StorageTypes::Table(0, 0, nullptr);

        table.AddColumn(column1);
        table.AddColumn(column2);

        auto* frameData = static_cast<object_t*>(allocator.AllocateRaw(Constants::PAGE_SIZE));
        auto frame = Pages::Frame(frameData);
        frame.Header()->pageId = 0;
        frame.Header()->size = 0;
        frame.Header()->bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;
        Pages::IndexPageView page(&frame);
        page.SetSubKeys(1);

        Errors::RuntimeStatus status;
        auto insertValues = DataStructures::PolymorphicArray<Value>(&allocator, 2);
        insertValues.Push(Value(1, &allocator));
        insertValues.Push(Value(DataTypes::StringView("Hello"), &allocator));

        // auto payload = table.CreateInsertPayload(status, &allocator, 0, 40, insertValues);

        int keyVal = 1;
        DataTypes::Indexing::Key key(&allocator, keyVal);
        // auto tuple = Pages::IndexInsertTuple(key, &payload);

        // constexpr static auto numTuples = 40;
        // for (int i = 0;i < numTuples; i++)
        // {
        //     if (i == numTuples - 1)
        //     {
        //         page.InsertTuple(tuple);
        //
        //     }
        //     else
        //         page.InsertTuple(tuple);
        // }


        for (int i = 0;i < page.PageSize(); i++){
            auto rid = CoreEngine::StorageTypes::RID(page.PageId(), i);

            // auto row = CoreEngine::StorageTypes::Table::Materialize(&allocator, &page, &rid);
            // std::cout << row << std::endl;
        }

        int diff = 0;

        // auto usedBytes = page.PageSize() * (tuple.payload->Size() + key.size + Pages::SlotDirectory::SIZE);

        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;

        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << Constants::INDEX_PAGE_DEFAULT_SIZE << std::endl;

        auto updates = DataStructures::PolymorphicArray<Value>(&allocator, 1);
        updates.Push(Value(DataTypes::StringView("Hello my name is bigger bro"), &allocator, 1));


        auto rid = CoreEngine::StorageTypes::RID(page.PageId(), 0);
        // auto row = table.Materialize(&allocator, &page, &rid);
        // row.Update(updates);

        // std::cout << "After update: " << row << std::endl;

        // auto newPayload = table.CreateInsertPayload(status, &allocator, 0, 40, row.Data());
        // tuple.payload = &newPayload;

        // const auto _ = page.UpdateRow(&allocator, *tuple.payload, 0);

        std::cout << "After update:" << std::endl;
        for (int i = 0;i < page.PageSize(); i++){
            auto rid_i = CoreEngine::StorageTypes::RID(page.PageId(), i);
            // auto updatedRow = table.Materialize(&allocator, &page, &rid_i);
            // std::cout << updatedRow << std::endl;
        }

        page.Defragment(&allocator);

        std::cout << "After Defragmentation:" << std::endl;
        for (int i = 0;i < page.PageSize(); i++){
            auto rid_i = CoreEngine::StorageTypes::RID(page.PageId(), i);
            // auto updatedRow = table.Materialize(&allocator, &page, &rid_i);
            // std::cout << updatedRow << std::endl;
        }

        // usedBytes = page.PageSize() * (tuple.payload->Size() + key.size + Pages::SlotDirectory::SIZE);

        // std::cout << "Used bytes: " << usedBytes << ", bytes left: " << page.BytesLeft() << std::endl;
        // std::cout << "Total page size: " << usedBytes + page.BytesLeft() << " = " << Constants::INDEX_PAGE_DEFAULT_SIZE << std::endl;
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

    void ParseJson(){
        const CoreEngine::Memory::Allocator allocator;
                // "scores": [85.5, 90.0, 78.0],
  //       "nestedArray": [
  //     "obj1"
  // ]

        //,
        // "nullValue": null
        const auto json = R"(
            {
                "name": "Alice",
                "age": 30,
                "isStudent": false,
                "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "nestedObject": {
                        "key": "value"
                    }
                }
            }
        )";

        Serialization::JsonParser parser(&allocator, json);
        const auto jsonBinary = parser.Parse();
        const auto jsonEntry = jsonBinary[DataTypes::StringView("address.street")];
        if (jsonEntry.Type() == Serialization::JsonType::Null)
            return;

        std::cout << jsonEntry.AsString() << std::endl;
    }
}
