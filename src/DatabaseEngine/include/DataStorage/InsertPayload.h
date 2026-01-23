#pragma once
#include "Errors.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

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

        inline Errors::RuntimeStatus SetTinyInt(const Value& value);
        inline Errors::RuntimeStatus SetSmallInt(const Value& value);
        inline Errors::RuntimeStatus SetInt(const Value& value);
        inline Errors::RuntimeStatus SetBigInt(const Value& value);
        inline Errors::RuntimeStatus SetDecimal(const Value& value);
        inline Errors::RuntimeStatus SetString(const Value& value);
        inline Errors::RuntimeStatus SetUnicodeString(const Value& value);
        inline Errors::RuntimeStatus SetBool(const Value& value);
        inline Errors::RuntimeStatus SetDateTime(const Value& value);
        inline Errors::RuntimeStatus SetGuid(const Value& value);

        Errors::RuntimeStatus SetDataByType(const Value& value, DataType type);

    public:

        InsertPayload();
        InsertPayload(object_t* payload, UnsignedSmallInt payloadSize);

        void SetData(const void* otherData, UnsignedSmallInt dataSize);
        void SetData(const void* otherData, UnsignedSmallInt dataSize, Int offSet);
        Errors::RuntimeStatus SetData(const Value& value, DataType type);

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
