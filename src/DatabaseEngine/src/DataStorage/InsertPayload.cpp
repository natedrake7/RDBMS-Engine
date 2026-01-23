#include "../../include/DataStorage/InsertPayload.h"

namespace DatabaseEngine::StorageTypes{
    Errors::RuntimeStatus InsertPayload::SetDataByType(const Value& value, const DataType type){
        switch (type) {
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

    Errors::RuntimeStatus InsertPayload::SetData(const Value& value, const DataType type){
        if (value.IsNull())
            return Errors::RuntimeStatus();

        return this->SetDataByType(value, type);
    }
}
