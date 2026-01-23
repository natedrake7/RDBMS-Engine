#include "../../include/DataStorage/InsertPayload.h"

#include "Converter.h"
#include "DataStorage/Column.h"

namespace DatabaseEngine::StorageTypes{
    Int InsertPayload::SetDataByType(const Value& value, const Column* column, Errors::RuntimeStatus& status){
        switch (column->GetColumnType()) {
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
            case DataType::UnicodeString:
                return this->SetString(value, column, status);
            case DataType::Bool:
                return this->SetBool(value, status);
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

    Int InsertPayload::SetTinyInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        TinyInt convertedValue;

        if (!Converter<TinyInt>::TryStoi(val, convertedValue)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for TinyInt";

            status.code = Errors::RuntimeError::Overflow;
            status.message = ss.str();
            return -1;
        }

        this->CopyToBuffer<TinyInt>(convertedValue);
        return sizeof(TinyInt);
    }

    Int InsertPayload::SetSmallInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        SmallInt convertedValue;

        if (!Converter<SmallInt>::TryStoi(val, convertedValue)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for SmallInt";

            status.code = Errors::RuntimeError::Overflow;
            status.message = ss.str();
            return -1;
        }

        this->CopyToBuffer<SmallInt>(convertedValue);
        return sizeof(SmallInt);
    }

    Int InsertPayload::SetInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();
        Int convertedValue;

        if (!Converter<Int>::TryStoi(val, convertedValue)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for Int";

            status.code = Errors::RuntimeError::Overflow;
            status.message = ss.str();
            return -1;
        }

        this->CopyToBuffer<Int>(convertedValue);
        return sizeof(Int);
    }

    Int InsertPayload::SetBigInt(const Value &value, Errors::RuntimeStatus& status){
        const auto val = value.AsBigInt();

        if (!Converter<BigInt>::TryStoi(val)) {
            std::ostringstream ss;

            ss << "Value " << val << " out of range for BigInt";

            status.message = ss.str();
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer<BigInt>(val);
        return sizeof(BigInt);
    }

    Int InsertPayload::SetDecimal(const Value &value, const Column* column, Errors::RuntimeStatus& status){
        const auto val = value.AsDecimal();
        const auto& columnHeader = column->GetColumnHeader();

        if (!Converter<DataTypes::Decimal>::TryStoi(val, column->GetColumnSize())) {
            std::ostringstream ss;

            ss  << "Value "
                << val << " out of range for Decimal("
                << columnHeader.precision << ","
                << columnHeader.scale << ")";

            status.message = ss.str();
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer(val);
        return val.GetRawDataSize();
    }

    Int InsertPayload::SetString(
        const Value &value,
        const Column* column,
        Errors::RuntimeStatus& status
    ){
        Errors::RuntimeStatus result;

        const auto val = value.AsString();
        const auto& columnHeader = column->GetColumnHeader();

        const auto size = val.size();
        if (size > columnHeader.recordSize) {
            std::ostringstream ss;

            ss  << "Value "
                << val << " out of range for String("
                << columnHeader.recordSize << ")";

            status.message = ss.str();
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer(val);
        return size;
    }

    Int InsertPayload::SetBool(const Value &value, Errors::RuntimeStatus& status){
        Errors::RuntimeStatus result;

        const auto val = value.AsBigInt();
        bool convertedValue;

        if (!Converter<bool>::TryStoi(val, convertedValue)) {
            std::ostringstream ss;
            ss << "Value " << val << " out of range for Bool";

            status.message = ss.str();
            status.code = Errors::RuntimeError::Overflow;
            return -1;
        }

        this->CopyToBuffer<bool>(convertedValue);
        return sizeof(bool);
    }

    Int InsertPayload::SetDateTime(const Value &value){
        this->CopyToBuffer(value.AsDateTime());
        return DATETIME_SIZE;
    }

    Int InsertPayload::SetGuid(const Value &value){
        this->CopyToBuffer(value.AsGuid());
        return DataTypes::GUID_SIZE;
    }

    InsertPayload::InsertPayload(){
        this->_data = nullptr;
        this->size = 0;
        this->offset = 0;
    }

    InsertPayload::InsertPayload(object_t* payload, const UnsignedSmallInt payloadSize){
        this->_data = payload;
        this->size = payloadSize;
        this->offset = 0;
    }

    void InsertPayload::SetData(const void* otherData, const UnsignedSmallInt dataSize){
        std::memcpy(this->_data + this->offset, otherData, dataSize);
        this->offset += dataSize;
    }

    Int InsertPayload::SetData(const Value& value, const Column* column, Errors::RuntimeStatus& status){
        if (value.IsNull())
            return 0;

        return this->SetDataByType(value, column, status);
    }
}
