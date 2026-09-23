#pragma once
#include "DataTypes/Value.h"
#include <vector>

#include "DataStructures/PolymorphicArray.h"

class MaterializedRow {
    DataStructures::PolymorphicArray<Value> data;

public:
    //TODO: to be removed later...
    MaterializedRow() = default;

    explicit MaterializedRow(const ::Memory::IAllocator* allocator);
    MaterializedRow(const MaterializedRow& other);
    MaterializedRow(MaterializedRow&& other) noexcept;

    void AddColumn(Value& field);
    void AddColumn(Value&& field);
    void AddColumn(const Value& field);
    void AddColumn(const Value& field, column_index_t columnIndex);
    void Print()const;
    [[nodiscard]] const DataStructures::PolymorphicArray<Value>& Data()const;
    [[nodiscard]] DataStructures::PolymorphicArray<Value>& Data();
    [[nodiscard]] Value GetColumnAt(Int columnPos)const;
    [[nodiscard]] const Value& GetColumnReferenceAt(Int columnPos)const;
    [[nodiscard]] Int GetSize()const;
    [[nodiscard]] Int GetByteSize()const;
    [[nodiscard]] Int GetPageByteSize()const;
    void SetColumnIndex(Int columnPos, column_index_t columnIndex);
    [[nodiscard]] BigInt ComputeHash()const;

    void Update(DataStructures::PolymorphicArray<Value>& updates);
    void Update(const DataStructures::PolymorphicArray<Value>& updates);
    void Update(Value& update);

    MaterializedRow& operator=(const MaterializedRow& other);
    MaterializedRow& operator=(MaterializedRow&& other) noexcept;

    friend bool operator==(const MaterializedRow& lhs, const MaterializedRow& rhs);
    friend std::ostream& operator<<(std::ostream& os, const MaterializedRow& result);
};
