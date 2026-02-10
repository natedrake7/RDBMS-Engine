#pragma once
#include "Row.h"
#include "../../Systemic/include/Errors.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/DataTypes/DateTime.h"
#include "../../../Systemic/include/DataTypes/Decimal.h"
#include "../../../Systemic/include/DataTypes/Guid.h"

namespace DatabaseEngine
{
    struct ExecutionProperties;
}

namespace Pages
{
    struct RawRowReference;
}

namespace DatabaseEngine::StorageTypes {
    class InsertPayload final{
        object_t* _data;
        UnsignedSmallInt size;
        UnsignedSmallInt offset;

        mutable RowHeader header;
        mutable bool isHeaderInitialized;
        mutable std::vector<Value> materializedColumns;

        bool isReferencingExternalData;

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

        page_offset_t DeserializeHeader(Int bitmapSize, Int numberOfColumns) const;

    public:
        InsertPayload();
        InsertPayload(
            const Memory::Allocator& allocator,
            UnsignedSmallInt size,
            UnsignedSmallInt startingOffset
        );
        InsertPayload(UnsignedSmallInt size, UnsignedSmallInt startingOffset);

        InsertPayload& operator=(InsertPayload&& other)noexcept;
        InsertPayload(InsertPayload&& other) noexcept;

        static InsertPayload FromRowPtr(const Pages::RawRowReference& rowPtr);

        // Copy operations perform deep copy to avoid double-free
        InsertPayload(const InsertPayload& other);
        InsertPayload& operator=(const InsertPayload& other);

        ~InsertPayload();

        void SetData(const void* otherData, UnsignedSmallInt dataSize);
        void SetData(const void* otherData, UnsignedSmallInt dataSize, Int offSet) const;
        Int SetData(const Value& value, const Column* column, Errors::RuntimeStatus& status);

        void AlignSizeWithOffset();

        Value MaterializeColumn(
            const ExecutionProperties& properties,
            const Column* column,
            Int numberOfColumns
        ) const;
        object_t* Data()const;
        UnsignedSmallInt Size()const;
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
