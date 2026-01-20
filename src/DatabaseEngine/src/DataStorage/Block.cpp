#include "../../include/DataStorage/Block.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Converter.h"

#include <cstring>
#include <iostream>

namespace DatabaseEngine::StorageTypes {
    Errors::RuntimeStatus Block::SetDataByType(const Value &value){
        switch (this->ColumnType()) {
        case DataType::TinyInt:
            return this->SetTinyInt(value);
        case DataType::SmallInt:
            return this->SetSmallInt(value);
        case DataType::Int:
            return this->SetInt(value);
        case DataType::BigInt:
            return this->SetBigInt(value);
        case DataType::Decimal:
            return this->SetDecimal(value);
        case DataType::String:
            return this->SetString(value);
        case DataType::UnicodeString:
            return this->SetUnicodeString(value);
        case DataType::Bool:
            return this->SetBool(value);
        case DataType::DateTime:
            return this->SetDateTime(value);
        case DataType::Guid:
            return this->SetGuid(value);
        case DataType::RowIdentifier:
        case DataType::Unknown:
        default:
            throw std::runtime_error("Invalid Datatype for column");
        }
    }

    Errors::RuntimeStatus Block::SetTinyInt(const Value &value){
        Errors::RuntimeStatus result;
        const auto val = value.AsBigInt();
        TinyInt convertedValue;

        if (!Converter<TinyInt>::TryStoi(val, convertedValue)) {
            ostringstream ss;

            ss << "Value " << val << " out of range for TinyInt";

            result.code = Errors::RuntimeError::Overflow;
            result.message = ss.str();
            return result;
        }

        this->CopyToBuffer<TinyInt>(convertedValue);
        return result;
    }

    Errors::RuntimeStatus Block::SetSmallInt(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();
        SmallInt convertedValue;

        if (!Converter<SmallInt>::TryStoi(val, convertedValue)) {
            ostringstream ss;

            ss << "Value " << val << " out of range for SmallInt";

            result.code = Errors::RuntimeError::Overflow;
            result.message = ss.str();
            return result;
        }

        this->CopyToBuffer<SmallInt>(convertedValue);
        return result;
    }

    Errors::RuntimeStatus Block::SetInt(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();
        Int convertedValue;

        if (!Converter<Int>::TryStoi(val, convertedValue)) {
            ostringstream ss;

            ss << "Value " << val << " out of range for Int";

            result.code = Errors::RuntimeError::Overflow;
            result.message = ss.str();
            return result;
        }

        this->CopyToBuffer<Int>(convertedValue);
        return result;
    }

    Errors::RuntimeStatus Block::SetBigInt(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();

        if (!Converter<BigInt>::TryStoi(val)) {
            ostringstream ss;

            ss << "Value " << val << " out of range for BigInt";

            result.message = ss.str();
            result.code = Errors::RuntimeError::Overflow;
            return result;
        }

        this->CopyToBuffer<BigInt>(val);
        return result;
    }

    Errors::RuntimeStatus Block::SetDecimal(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsDecimal();
        const auto& columnHeader = this->column->GetColumnHeader();

        if (!Converter<DataTypes::Decimal>::TryStoi(val, this->column->GetColumnSize())) {
            ostringstream ss;

            ss  << "Value "
                << val << " out of range for Decimal("
                << columnHeader.precision << ","
                << columnHeader.scale << ")";

            result.message = ss.str();
            result.code = Errors::RuntimeError::Overflow;
            return result;
        }

        this->CopyToBuffer(val);
        return result;
    }

    Errors::RuntimeStatus Block::SetString(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsString();
        const auto& columnHeader = this->column->GetColumnHeader();

        if (val.size() > columnHeader.recordSize) {
            ostringstream ss;

            ss  << "Value "
                << val << " out of range for String("
                << columnHeader.recordSize << ")";

            result.message = ss.str();
            result.code = Errors::RuntimeError::Overflow;
            return result;
        }

        this->CopyToBuffer(val);
        return result;
    }

    Errors::RuntimeStatus Block::SetUnicodeString(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsUnicodeString();
        const auto& columnHeader = this->column->GetColumnHeader();

        if (val.size() > columnHeader.recordSize) {
            result.code = Errors::RuntimeError::Overflow;

            ostringstream ss;

            // ss  << "Value "
            //     << val << " out of range for UString("
            //     << columnHeader.recordSize << ")";

            result.message = ss.str();
            return result;
        }

        this->CopyToBuffer(val);
        return result;
    }

    Errors::RuntimeStatus Block::SetBool(const Value &value){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();
        bool convertedValue;

        if (!Converter<bool>::TryStoi(val, convertedValue)) {
            ostringstream ss;
            ss << "Value " << val << " out of range for Bool";

            result.message = ss.str();
            result.code = Errors::RuntimeError::Overflow;
            return result;
        }

        this->CopyToBuffer<bool>(convertedValue);
        return result;
    }

    Errors::RuntimeStatus Block::SetDateTime(const Value &value){
        Errors::RuntimeStatus result;

        this->CopyToBuffer(value.AsDateTime());
        return result;
    }

    Errors::RuntimeStatus Block::SetGuid(const Value &value){
        Errors::RuntimeStatus result;

        this->CopyToBuffer(value.AsGuid());
        return result;
    }

    Block::Block() {
        this->size = 0;
        this->column = nullptr;
        this->data = nullptr;
    }

    Block::Block(const Column* column)
    {
        this->size = 0;
        this->column = column;
        this->data = nullptr;
        // this->SetData(data, size);
    }

    Block::Block(const Block *block)
    {
        this->size = block->size;
        this->column = block->column;
        this->data = nullptr;
        this->SetData(block->data, block->size);
    }

    Block::Block(const void* data, const block_size_t size, const Column* column)
    {
        this->size = size;
        this->column = column;
        this->data = nullptr;
        this->SetData(data, size);
    }

    Block::Block(object_t *data, const block_size_t size, const Column *column){
        this->size = size;
        this->column = column;
        this->data = data;
    }

    Block::~Block()
    {
        std::free(this->data);
        this->data = nullptr;
    }

    void Block::SetData(const void* inputData, const block_size_t inputSize)
    {
        std::free(this->data);
        this->data = nullptr;

        if (inputData == nullptr)
        {
            this->size = 0;
            return;
        }

        this->data = new object_t[inputSize];
        memcpy(this->data, inputData, inputSize);

        this->size = inputSize;
    }

    Errors::RuntimeStatus Block::SetData(const Value &value){
        std::free(this->data);
        this->data = nullptr;

        if (value.IsNull()) {
            this->size = 0;
            return {};
        }

        return this->SetDataByType(value);
    }

    object_t* Block::Data() const { return this->data; }

    block_size_t Block::Size() const { return this->size; }

    bool Block::AsBool() const { return *reinterpret_cast<bool*>(this->data); }

    TinyInt Block::AsTinyInt() const { return *reinterpret_cast<TinyInt*>(this->data); }

    SmallInt Block::AsSmallInt() const { return *reinterpret_cast<SmallInt*>(this->data); }

    Int Block::AsInt() const { return *reinterpret_cast<Int*>(this->data); }

    BigInt Block::AsBigInt() const { return *reinterpret_cast<BigInt*>(this->data); }

    std::string Block::AsString() const { return { reinterpret_cast<char*>(this->data), static_cast<size_t>(this->size) };}

    u16string Block::AsUnicodeString() const { return { reinterpret_cast<char16_t*>(this->data), static_cast<size_t>(this->size / 2) }; }

    DataTypes::Decimal Block::AsDecimal() const { return DataTypes::Decimal(this->data, this->size); }

    DataTypes::DateTime Block::AsDateTime() const { return DataTypes::DateTime(*reinterpret_cast<time_t*>(this->data)); }

    DataTypes::Guid Block::AsGuid() const{  return { this->data, this->size }; }

    Pages::DataObjectPointer Block::AsLargeObjectPointer() const { return *reinterpret_cast<Pages::DataObjectPointer*>(this->data); }

    Pages::OverflowPointer Block::AsOverflowPointer() const { return *reinterpret_cast<Pages::OverflowPointer*>(this->data); }

    column_index_t Block::ColumnIndex() const { return this->column->GetColumnIndex(); }

    row_size_t Block::ColumnSize() const { return this->column->GetColumnSize(); }

    DataType Block::ColumnType() const { return this->column->GetColumnType(); }

    bool Block::Null() const{ return this->data == nullptr; }

    const Column * Block::GetColumn() const{ return this->column; }

    void Block::SetColumn(const Column* otherColumn) { this->column = otherColumn; };
}