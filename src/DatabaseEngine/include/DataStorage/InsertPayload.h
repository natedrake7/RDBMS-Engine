#pragma once
#include "Errors.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace DatabaseEngine::StorageTypes
{
    class Column;
}

namespace DatabaseEngine::StorageTypes {
    class InsertPayload final{
        object_t* _data;
        UnsignedSmallInt size;
        UnsignedSmallInt offset;

        template <typename T>
        void CopyToBuffer(T value);
        inline void CopyToBuffer(const std::string& src);
        inline void CopyToBuffer(const DataTypes::Decimal& src);
        inline void CopyToBuffer(const DataTypes::DateTime& src);
        inline void CopyToBuffer(const DataTypes::Guid& src);

        inline Int SetTinyInt(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetSmallInt(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetInt(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetBigInt(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetDecimal(const Value& value, const Column* column, Errors::RuntimeStatus& status);
        inline Int SetString(const Value& value, const Column* column, Errors::RuntimeStatus& status);
        inline Int SetBool(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetDateTime(const Value& value);
        inline Int SetGuid(const Value& value);

        Int SetDataByType(const Value& value, const Column* column, Errors::RuntimeStatus& status);

    public:

        InsertPayload();
        InsertPayload(object_t* payload, UnsignedSmallInt payloadSize);

        void SetData(const void* otherData, UnsignedSmallInt dataSize);
        void SetData(const void* otherData, UnsignedSmallInt dataSize, Int offSet);
        Int SetData(const Value& value, const Column* column, Errors::RuntimeStatus& status);

    };

    template <typename T>
    void InsertPayload::CopyToBuffer(T value){
        this->SetData(&value, sizeof(T));
    }

    void InsertPayload::CopyToBuffer(const std::string &src) {
        this->SetData(src.data(), src.size());
    }

    void InsertPayload::CopyToBuffer(const DataTypes::Decimal &src){
        this->SetData(src.GetRawData(), src.GetRawDataSize());
    }

    void InsertPayload::CopyToBuffer(const DataTypes::Guid &src){
        this->SetData(src.GetData().data(), DataTypes::GUID_SIZE);
    }

    void InsertPayload::CopyToBuffer(const DataTypes::DateTime &src){
        const auto dt = src.GetUnixTimeStamp();
        this->SetData(&dt, DataTypes::DateTime::Size());
    }
}
