#include "../../include/DataStorage/InsertPayload.h"

#include <cmath>
#include <iostream>

#include "Converter.h"
#include "Contexts/ExecutionContext.h"
#include "DataStorage/Column.h"
#include "DataStorage/Row.h"
#include "Memory/IAllocator.h"
#include "Pages/Additional/RawRowReference.h"
#include "../../../Systemic/include/DataTypes/JsonBinary.h"

namespace CoreEngine::StorageTypes{
    Int InsertPayload::SetTinyInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        TinyInt convertedValue;

        if (!Converter::TryDownCast<TinyInt>(val, convertedValue)) {
            std::ostringstream ss;
            ss << "Value " << val << " out of range for TinyInt";

            status.code = Errors::RuntimeError::Overflow;
            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            return -1;
        }

        this->CopyToBuffer<TinyInt>(convertedValue);
        return sizeof(TinyInt);
    }

    Int InsertPayload::SetSmallInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        SmallInt convertedValue;

        if (!Converter::TryDownCast<SmallInt>(val, convertedValue)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for SmallInt";

            status.code = Errors::RuntimeError::Overflow;
            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            return -1;
        }

        this->CopyToBuffer<SmallInt>(convertedValue);
        return sizeof(SmallInt);
    }

    Int InsertPayload::SetInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        Int convertedValue;

        if (!Converter::TryDownCast<Int>(val, convertedValue)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for Int";

            status.code = Errors::RuntimeError::Overflow;
            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            return -1;
        }

        this->CopyToBuffer<Int>(convertedValue);
        return sizeof(Int);
    }

    Int InsertPayload::SetBigInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();

        if (!Converter::TryDownCast<BigInt>(val)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for BigInt";

            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer<BigInt>(val);
        return sizeof(BigInt);
    }

    Int InsertPayload::SetDecimal(const Value &value, const Column* column, Errors::RuntimeStatus& status){
        const auto val = value.AsDecimal();
        const auto& columnHeader = column->GetColumnHeader();

        if (!Converter::DecimalFitsInSize(val, column->Size())) {
            std::ostringstream ss;

            ss  << "Value "
                << val << " out of range for Decimal("
                << columnHeader.precision << ","
                << columnHeader.scale << ")";

            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer(val);
        return val.RawSize();
    }

    Int InsertPayload::SetString(
        const Value &value,
        const Column* column,
        Errors::RuntimeStatus& status
    ){
        Errors::RuntimeStatus result;

        const auto val = value.AsString();
        const auto& columnHeader = column->GetColumnHeader();

        const auto strSize = val.Size();
        if (strSize > columnHeader.recordSize) {
            std::ostringstream ss;

            ss  << "Value "
                << val << " out of range for String("
                << columnHeader.recordSize << ")";

            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer(val);
        return strSize;
    }

    Int InsertPayload::SetJson(const Value& value){
        const auto jsonBinary = value.AsJson();
        this->CopyToBuffer(jsonBinary);
        return jsonBinary.Size();
    }

    Int InsertPayload::SetBool(const Value &value, Errors::RuntimeStatus& status){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();
        bool convertedValue;

        if (!Converter::TryDownCast<bool>(val, convertedValue)) {
            std::ostringstream ss;
            ss << "Value " << val << " out of range for Bool";

            status.message = DataTypes::String(ss.str(), value.GetAllocator());
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer<bool>(convertedValue);
        return sizeof(bool);
    }

    Int InsertPayload::SetDateTime(const Value &value){
        this->CopyToBuffer(value.AsDateTime());
        return sizeof(DataTypes::DateTime);
    }

    Int InsertPayload::SetGuid(const Value &value){
        this->CopyToBuffer(value.AsGuid());
        return DataTypes::GUID_SIZE;
    }

    Int InsertPayload::SetDataByType(const Value& value, const Column* column, Errors::RuntimeStatus& status){
        switch (column->Type()) {
        case DataType::TinyInt:
            return this->SetTinyInt(value, status);
        case DataType::SmallInt:
            return this->SetSmallInt(value, status);
        case DataType::Int:
            return this->SetInt(value, status);
        case DataType::BigInt:
            return this->SetBigInt(value, status);
        case DataType::Decimal:
            return this->SetDecimal(value, column, status);
        case DataType::String:
            return this->SetString(value, column, status);
        case DataType::Json:
            return this->SetJson(value);
        case DataType::Bool:
            return this->SetBool(value, status);
        case DataType::DateTime:
            return this->SetDateTime(value);
        case DataType::Guid:
            return this->SetGuid(value);
        case DataType::RowIdentifier:
        case DataType::Null:
        default:
            throw std::runtime_error("Invalid Datatype for column");
        }
    }

    InsertPayload::InsertPayload(){
        this->_data = nullptr;
        this->size = 0;
        this->offset = 0;
    }

    InsertPayload::InsertPayload(
        const ::Memory::IAllocator* allocator,
        const UnsignedSmallInt size,
        const UnsignedSmallInt startingOffset
    ){
        this->_data = static_cast<object_t*>(allocator->AllocateRaw(size));
        this->size = size;
        this->offset = startingOffset;
    }

    InsertPayload::InsertPayload(const UnsignedSmallInt size, const UnsignedSmallInt startingOffset){
        this->_data = static_cast<object_t*>(std::malloc(size));
        this->size = size;
        this->offset = startingOffset;
    }

    InsertPayload& InsertPayload::operator=(InsertPayload&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->size = other.size;
        this->offset = other.offset;

        other._data = nullptr;
        other.size = 0;
        other.offset = 0;

        return *this;
    }

    InsertPayload::InsertPayload(InsertPayload&& other) noexcept{
        this->_data = other._data;
        this->size = other.size;
        this->offset = other.offset;

        other._data = nullptr;
        other.size = 0;
        other.offset = 0;
    }

    InsertPayload InsertPayload::FromRowPtr(const Pages::RawRowReference& rowPtr){
        auto payload = InsertPayload();

        payload._data = rowPtr._data;
        payload.size = rowPtr.size;
        payload.offset = 0;

        return payload;
    }

    // InsertPayload::InsertPayload(const InsertPayload& other){
    //     if (other._data != nullptr) {
    //         this->_data = static_cast<object_t*>(std::malloc(other.size));
    //         std::memcpy(this->_data, other._data, other.size);
    //     }
    //     else
    //         this->_data = nullptr;
    //
    //     this->size = other.size;
    //     this->offset = other.offset;
    //     this->isHeaderInitialized = other.isHeaderInitialized;
    //     this->header = other.header;
    //     this->isReferencingExternalData = other.isReferencingExternalData;
    // }
    //
    // InsertPayload& InsertPayload::operator=(const InsertPayload& other){
    //     if (this == &other)
    //         return *this;
    //
    //     // Free existing data to prevent memory leak
    //     std::free(this->_data);
    //
    //     // Deep copy: allocate new memory and copy contents
    //     if (other._data != nullptr) {
    //         this->_data = static_cast<object_t*>(std::malloc(other.size));
    //         std::memcpy(this->_data, other._data, other.size);
    //     }
    //     else
    //         this->_data = nullptr;
    //
    //     this->size = other.size;
    //     this->offset = other.offset;
    //     this->isHeaderInitialized = other.isHeaderInitialized;
    //     this->header = other.header;
    //     this->isReferencingExternalData = other.isReferencingExternalData;
    //
    //     return *this;
    // }

    UnsignedSmallInt InsertPayload::SetData(const void* otherData, const UnsignedSmallInt dataSize){
        std::memcpy(this->_data + this->offset, otherData, dataSize);
        const auto dataOffset = this->offset;
        this->offset += dataSize;
        return dataOffset;
    }

    void InsertPayload::SetData(const void* otherData, const UnsignedSmallInt dataSize, const Int offSet) const{
        std::memcpy(this->_data + offSet, otherData, dataSize);
    }

    Int InsertPayload::SetData(const Value& value, const Column* column, Errors::RuntimeStatus& status){
        return this->SetDataByType(value, column, status);
    }

    void InsertPayload::AlignSizeWithOffset(){
        this->size = this->offset;
    }

    Value InsertPayload::MaterializeColumn(
        const ExecutionContext& context,
        const Column* column
    ) const{
        // Calculate bitmap size once
        const auto columnOrdinal = column->OrdinalPosition();

        const auto offSet = sizeof(RowHeader) + columnOrdinal * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);

        if (columnDataEntry->Type() == RowEntry::NULLVAL)
            return Value::Null(context.GetAllocator());

        // Create and populate the value
        return Value::FromExternalStorage(
            this->_data + columnDataEntry->Offset(),
            columnDataEntry->Size(),
            column->Type(),
            context.GetAllocator(),
            columnOrdinal
        );
    }

    object_t* InsertPayload::Data() const{
        return this->_data;
    }

    UnsignedSmallInt InsertPayload::Size() const{
        return this->size;
    }

    UnsignedSmallInt InsertPayload::Offset() const{
        return this->offset;
    }
}
