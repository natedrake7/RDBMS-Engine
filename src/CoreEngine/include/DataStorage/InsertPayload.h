#pragma once
#include "Row.h"
#include "../../Systemic/include/Errors.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/DataTypes/DateTime.h"
#include "../../../Systemic/include/DataTypes/Decimal.h"
#include "../../../Systemic/include/DataTypes/Guid.h"
#include "../../../Systemic/include/DataTypes/JsonBinary.h"

namespace Expressions
{
    class Expression;
}

namespace CoreEngine{
    class ExecutionContext;
}

namespace Pages{
    struct RawRowReference;
}

namespace CoreEngine::StorageTypes {
    class Column;

    struct InsertSlot{
        enum class SlotKind: UnsignedTinyInt{
            Value = 0,
            Default = 1,
            Null = 2
        };

        column_index_t _slot;
        SlotKind _kind;

        InsertSlot()
            : _slot(0), _kind(SlotKind::Null) {}
        explicit InsertSlot(const SlotKind kind)
            : _slot(0), _kind(kind) {}
        InsertSlot(const column_index_t slot, const SlotKind kind)
            : _slot(slot), _kind(kind) {}

        static InsertSlot ValueSlot(const column_index_t slot){
            return InsertSlot(slot, SlotKind::Value);
        }
        static InsertSlot DefaultSlot(const column_index_t slot){
            return InsertSlot(slot, SlotKind::Default);
        }
        static InsertSlot NullSlot(){
            return InsertSlot(SlotKind::Null);
        }
    };

    struct InsertPlan{
        DataStructures::PolymorphicArray<InsertSlot> _slotMap;
        DataStructures::PolymorphicArray<Expressions::Expression*> _sharedDefaults;

        InsertPlan() = default;
        InsertPlan(
            DataStructures::PolymorphicArray<InsertSlot>&& slotMap,
            DataStructures::PolymorphicArray<Expressions::Expression*>&& sharedDefaults
        ) : _slotMap(std::move(slotMap)), _sharedDefaults(std::move(sharedDefaults)) {}

        InsertPlan(InsertPlan&& other) noexcept = default;
        InsertPlan& operator=(InsertPlan&& other) noexcept = default;
    };

    class InsertPayload final{
        object_t* _data;
        UnsignedInt size;
        UnsignedInt offset;

        template <typename T>
        void CopyToBuffer(T value);
        inline void CopyToBuffer(const DataTypes::JsonBinary& src);
        inline void CopyToBuffer(const DataTypes::String& src);
        inline void CopyToBuffer(const char* src, Int srcSize);
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
        inline Int SetJson(const Value& value);
        inline Int SetBool(const Value& value, Errors::RuntimeStatus& status);
        inline Int SetDateTime(const Value& value);
        inline Int SetGuid(const Value& value);

        Int SetDataByType(const Value& value, const Column* column, Errors::RuntimeStatus& status);

    public:
        InsertPayload();
        InsertPayload(
            const ::Memory::IAllocator* allocator,
            UnsignedSmallInt size,
            UnsignedSmallInt startingOffset
        );
        InsertPayload(UnsignedSmallInt size, UnsignedSmallInt startingOffset);

        InsertPayload& operator=(InsertPayload&& other)noexcept;
        InsertPayload(InsertPayload&& other) noexcept;

        static InsertPayload FromRowPtr(const Pages::RawRowReference& rowPtr);

        UnsignedSmallInt SetData(const void* otherData, UnsignedSmallInt dataSize);
        void SetData(const void* otherData, UnsignedSmallInt dataSize, Int offSet) const;
        Int SetData(const Value& value, const Column* column, Errors::RuntimeStatus& status);

        void AlignSizeWithOffset();

        Value MaterializeColumn(
            const ExecutionContext& context,
            const Column* column
        ) const;
        object_t* Data()const;
        UnsignedSmallInt Size()const;
        UnsignedSmallInt Offset()const;
    };

    template <typename T>
    void InsertPayload::CopyToBuffer(T value){
        this->SetData(&value, sizeof(T));
    }

    void InsertPayload::CopyToBuffer(const DataTypes::JsonBinary& src){
        this->SetData(src.Data(), src.Size());
    }

    void InsertPayload::CopyToBuffer(const DataTypes::String& src){
        this->SetData(src.Data(), src.Size());
    }

    void InsertPayload::CopyToBuffer(const char* src, const Int srcSize){
        this->SetData(src, srcSize);
    }

    void InsertPayload::CopyToBuffer(const std::string &src) {
        this->SetData(src.data(), src.size());
    }

    void InsertPayload::CopyToBuffer(const DataTypes::Decimal &src){
        this->SetData(src.RawData(), src.RawSize());
    }

    void InsertPayload::CopyToBuffer(const DataTypes::Guid &src){
        this->SetData(src.GetData(), DataTypes::GUID_SIZE);
    }

    void InsertPayload::CopyToBuffer(const DataTypes::DateTime &src){
        const auto dt = src.UnixTimeStamp();
        this->SetData(&dt, sizeof(DataTypes::DateTime));
    }
}
