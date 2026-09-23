#include "../include/MaterializedRow.h"

#include <iostream>

#include "../include/DataTypes/DateTime.h"
#include "../include/DataTypes/Decimal.h"

MaterializedRow::MaterializedRow(const ::Memory::IAllocator* allocator)
    : data(allocator) {}

MaterializedRow::MaterializedRow(const MaterializedRow& other) {
    this->data = other.data;
}

MaterializedRow::MaterializedRow(MaterializedRow&& other) noexcept {
    this->data = std::move(other.data);
}

void MaterializedRow::AddColumn(Value &field) {
    this->data.Push(std::move(field));
}

void MaterializedRow::AddColumn(Value&& field) {
    this->data.Push(std::move(field));
}

void MaterializedRow::AddColumn(const Value& field) {
    this->data.Push(field);
}

void MaterializedRow::AddColumn(const Value& field, const column_index_t columnIndex) {
    this->data.Insert(field, columnIndex);
}

void MaterializedRow::Print() const {
    for (int i = 0; i < this->data.Size(); ++i) {
        const auto& column = this->data[i];

        if (
            column.Data() == nullptr || column.Size() == 0
        ) {
            std::cout << "NULL"
                      << ((i == this->data.Size() - 1) ? "\n" : " || ");
            continue;
        }

        switch (column.GetType()) {
            case DataType::TinyInt:
                std::cout << static_cast<SmallInt>(column.AsTinyInt());
                break;
            case DataType::SmallInt:
                std::cout << column.AsSmallInt();
                break;
            case DataType::Int:
                std::cout << column.AsInt();
                break;
            case DataType::BigInt:
                std::cout << column.AsBigInt();
                break;
            case DataType::Decimal:
                std::cout << column.AsDecimal();
                break;
            case DataType::String:
                std::cout << column.AsStringView();
                break;
            case DataType::Bool:
                std::cout << (column.AsBool() ? "TRUE" : "FALSE");
                break;
            case DataType::DateTime:
                column.AsDateTime().Print(std::cout);
                break;
            case DataType::Guid:
                std::cout << column.AsGuid();
                break;
            default:
                break;
        }

        std::cout << ((i == this->data.Size() - 1) ? "\n" : " || ");
    }
}

const DataStructures::PolymorphicArray<Value>& MaterializedRow::Data() const { return this->data; }

DataStructures::PolymorphicArray<Value>& MaterializedRow::Data() {
    return this->data;
}

Value MaterializedRow::GetColumnAt(const Int columnPos) const {
    return this->data[columnPos];
}

const Value& MaterializedRow::GetColumnReferenceAt(const Int columnPos) const {
    return this->data[columnPos];
}

int MaterializedRow::GetSize() const { return this->data.Size(); }

Int MaterializedRow::GetByteSize() const {
    Int totalSize = 0;
    for (const auto& value : this->data) {
        totalSize += sizeof(block_size_t); // size of block
        totalSize += sizeof(DataType);     // type of block
        totalSize += value.Size();         // data size
    }
    return totalSize;
}

Int MaterializedRow::GetPageByteSize() const {
    Int totalSize = 0;
    for (const auto& value : this->data) {
        totalSize += sizeof(block_size_t); // size of block
        totalSize += value.Size();         // data size
    }
    return totalSize;
}

void MaterializedRow::SetColumnIndex(const Int columnPos, const column_index_t columnIndex) {
    if (columnPos >= this->data.Size())
        return;

    this->data[columnPos].SetColumnIndex(columnIndex);
}

int64_t MaterializedRow::ComputeHash() const {
    std::hash<std::string> strHash;
    size_t seed = 0;
    // for (const auto& value : this->data) {
    //     auto str = value.AsString();
    //     seed ^= strHash(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    // }
    return static_cast<int64_t>(seed);
}

void MaterializedRow::Update(DataStructures::PolymorphicArray<Value>& updates) {
    for (auto& value : updates) {
        auto& otherValue = this->data[value.GetColumnIndex()];
        otherValue = std::move(value);
    }
}

void MaterializedRow::Update(const DataStructures::PolymorphicArray<Value>& updates) {
    for (auto& value : updates) {
        auto& otherValue = this->data[value.GetColumnIndex()];
        otherValue = value;
    }
}

void MaterializedRow::Update(Value& update) {
    const auto index = update.GetColumnIndex();
    this->data[index] = std::move(update);
}

MaterializedRow& MaterializedRow::operator=(const MaterializedRow& other) {
    if (this == &other)
        return *this;

    this->data = other.data;
    return *this;
}

MaterializedRow& MaterializedRow::operator=(MaterializedRow&& other) noexcept {
    if (this == &other)
        return *this;

    this->data = std::move(other.data);
    return *this;
}

bool operator==(const MaterializedRow& lhs, const MaterializedRow& rhs) {
    if (lhs.GetSize() != rhs.GetSize())
        return false;

    for (int i = 0; i < lhs.data.Size(); i++) {
        if (lhs.data[i] != rhs.data[i])
            return false;
    }

    return true;
}

std::ostream& operator<<(std::ostream& os, const MaterializedRow& result) {
    for (int i = 0; i < result.data.Size(); ++i) {
        const auto& column = result.data[i];

        if (column.Data() == nullptr || column.Size() == 0) {
            os << "NULL"
               << ((i == result.data.Size() - 1) ? "\n" : " || ");
            continue;
        }

        switch (column.GetType()) {
            case DataType::TinyInt:
                os << static_cast<int16_t>(column.AsTinyInt());
                break;
            case DataType::SmallInt:
                os << column.AsSmallInt();
                break;
            case DataType::Int:
                os << column.AsInt();
                break;
            case DataType::BigInt:
                os << column.AsBigInt();
                break;
            case DataType::Decimal:
                os << column.AsDecimal();
                break;
            case DataType::String:
            case DataType::Json:
                os << column.AsStringView();
                break;
            case DataType::Bool:
                os << (column.AsBool() ? "TRUE" : "FALSE");
                break;
            case DataType::DateTime:
                column.AsDateTime().Print(os);
                break;
            case DataType::Guid:
                os << column.AsGuid();
                break;
            default:
                break;
        }

        os << ((i == result.data.Size() - 1) ? "\n" : " || ");
    }

    return os;
}
