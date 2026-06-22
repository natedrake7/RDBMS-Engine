#pragma once
#include <string>
#include <type_traits>

#include "DataTypes.h"
#include "StringView.h"
#include "DateTime.h"
#include "Decimal.h"
#include "JsonBinary.h"
#include "Guid.h"
#include "../Serialization/JsonParser.h"

namespace Memory{
    class IAllocator;
}

class Value{
    const object_t* data;

    const Memory::IAllocator* _allocator;

    block_size_t size;
    column_index_t columnIndex;
    DataType type;

    [[nodiscard]] long double InterpolateString() const;

    static void BinaryOperationException(DataType lhs, DataType rhs);

    explicit Value(
        object_t* data,
        Int size,
        DataType type,
        const Memory::IAllocator* allocator,
        column_index_t index = 0
    );

    public:
        Value(column_index_t index = 0);
        Value(const Value& other);
        Value& operator=(const Value& other);
        Value(Value&& other)noexcept;
        Value& operator=(Value&& other) noexcept;

        explicit Value(const ::Memory::IAllocator* allocator, column_index_t index = 0);
        explicit Value(
            const object_t* data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        Value(const std::string& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::StringView& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const Serialization::JsonValue& data, const Memory::IAllocator* allocator, column_index_t index = 0);

        template<DataTypes::Primitive T>
        Value(
            T other,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        template<DataTypes::NonPrimitiveType T>
        Value(
            T& other,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        template<DataTypes::NonPrimitiveType T>
        Value(
            const T& other,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value FromMove(
            object_t*  data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value FromExternalStorage(
            const object_t* data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value Null(column_index_t columnIndex = 0);
        static Value Null(const Memory::IAllocator* allocator, column_index_t columnIndex = 0);

        void SetColumnIndex(column_index_t otherIndex);
        void SetType(DataType otherType);

        void SetNull();

        template<DataTypes::Primitive T>
        void Set(T other);

        template<DataTypes::NonPrimitiveType T>
        void Set(T& other);

        template<DataTypes::NonPrimitiveType T>
        void Set(const T& other);

        [[nodiscard]] block_size_t Size() const;
        [[nodiscard]] const object_t* Data() const;

        [[nodiscard]] bool IsNull() const;
        [[nodiscard]] column_index_t GetColumnIndex() const;
        [[nodiscard]] DataType GetType() const;

        [[nodiscard]] bool AsBool()const;
        [[nodiscard]] TinyInt AsTinyInt()const;
        [[nodiscard]] SmallInt AsSmallInt()const;
        [[nodiscard]] Int AsInt()const;
        [[nodiscard]] BigInt AsBigInt()const;
        [[nodiscard]] DataTypes::String AsString()const;
        [[nodiscard]] std::string AsStdString()const;
        [[nodiscard]] DataTypes::StringView AsStringView()const;
        [[nodiscard]] DataTypes::Decimal AsDecimal()const;
        [[nodiscard]] DataTypes::DateTime AsDateTime()const;
        [[nodiscard]] time_t AsUnixTimeStamp() const;
        [[nodiscard]] DataTypes::Guid AsGuid()const;
        [[nodiscard]] DataTypes::JsonBinary AsJson()const;
        [[nodiscard]] page_id_t AsLargeObjectPointer() const;

        friend std::ostream& operator<<(std::ostream& os, const Value& field);

        [[nodiscard]] const Memory::IAllocator* GetAllocator() const;

        [[nodiscard]] bool ParseAsBoolFromString()const;

        [[nodiscard]] long double Interpolate()const;

        template<typename T>
        [[nodiscard]] T Get() const;

        friend bool operator<(const Value& lhs, const Value& rhs);
        friend bool operator==(const Value& lhs, const Value& rhs);
        friend bool operator>(const Value& lhs, const Value& rhs);
        friend bool operator<=(const Value& lhs, const Value& rhs);
        friend bool operator>=(const Value& lhs, const Value& rhs);
        friend bool operator!=(const Value& lhs, const Value& rhs);
};

template <DataTypes::Primitive T>
Value::Value(const T other, const Memory::IAllocator* allocator, const column_index_t index){
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataTypes::DataTypeOf<T>();
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(sizeof(T)));
    std::memcpy(buffer, &other, sizeof(T));
    this->data = buffer;
    this->size = sizeof(T);
}

template <DataTypes::NonPrimitiveType T>
Value::Value(T& other, const Memory::IAllocator* allocator, const column_index_t index){
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataTypes::DataTypeOf<T>();

    if constexpr (DataTypes::IsString<T>){
        this->data = reinterpret_cast<const object_t*>(other.Data());
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsJson<T>){
        this->data = other.Data();
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsDecimal<T>){
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.RawSize()));
        std::memcpy(buffer, other.RawData(), other.RawSize());
        this->data = buffer;
        this->size = other.RawSize();
    }
    else
        static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");
}

template <DataTypes::NonPrimitiveType T>
Value::Value(const T& other, const Memory::IAllocator* allocator, column_index_t index){
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataTypes::DataTypeOf<T>();

    if constexpr (DataTypes::IsString<T>){
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.Size()));
        std::memcpy(buffer, other.Data(), other.Size());
        this->data = buffer;
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsJson<T>){
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.Size()));
        std::memcpy(buffer, other.Data(), other.Size());
        this->data = buffer;
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsDecimal<T>){
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.RawSize()));
        std::memcpy(buffer, other.RawData(), other.RawSize());
        this->data = buffer;
        this->size = other.RawSize();
    }
    else
        static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");
}

template <DataTypes::Primitive T>
void Value::Set(const T other){
    auto* buffer = static_cast<object_t*>(_allocator->AllocateRaw(sizeof(T)));
    std::memcpy(buffer, &other, sizeof(T));
    this->data = buffer;
    this->size = sizeof(T);
    this->type = DataTypes::DataTypeOf<T>();
}

template <DataTypes::NonPrimitiveType T>
void Value::Set(T& other){
    if constexpr (DataTypes::IsString<T>){
        this->data = reinterpret_cast<const object_t*>(other.Data());
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsJson<T>){
        this->data = other.Data();
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsDecimal<T>){
        this->data = other.RawData();
        this->size = other.RawSize();
    }
    else
        static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");
}

template <DataTypes::NonPrimitiveType T>
void Value::Set(const T& other){
    if constexpr (DataTypes::IsString<T>){
        auto* buffer = static_cast<object_t*>(_allocator->AllocateRaw(other.Size()));
        std::memcpy(buffer, other.Data(), other.Size());
        this->data = buffer;
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsJson<T>){
        auto* buffer = static_cast<object_t*>(_allocator->AllocateRaw(other.Size()));
        std::memcpy(buffer, other.Data(), other.Size());
        this->data = buffer;
        this->size = other.Size();
    }
    else if constexpr (DataTypes::IsDecimal<T>){
        auto* buffer = static_cast<object_t*>(_allocator->AllocateRaw(other.RawSize()));
        std::memcpy(buffer, other.RawData(), other.RawSize());
        this->data = buffer;
        this->size = other.RawSize();
    }
    else
        static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");
}

// Typed unbox: maps a compile-time T to the matching runtime accessor. The
// discarded `if constexpr` branches are never instantiated, so forward-declared
// return types (Decimal/DateTime/Guid) only need to be complete in the TU that
// actually instantiates that branch.
template <typename T>
T Value::Get() const{
    if constexpr (std::is_same_v<T, bool>)
        return this->AsBool();
    else if constexpr (std::is_same_v<T, TinyInt>)
        return this->AsTinyInt();
    else if constexpr (std::is_same_v<T, SmallInt>)
        return this->AsSmallInt();
    else if constexpr (std::is_same_v<T, Int>)
        return this->AsInt();
    else if constexpr (std::is_same_v<T, BigInt>)
        return this->AsBigInt();
    else if constexpr (std::is_same_v<T, DataTypes::DateTime>)
        return this->AsDateTime();
    else if constexpr (std::is_same_v<T, DataTypes::Guid>)
        return this->AsGuid();
    else if constexpr (std::is_same_v<T, DataTypes::Decimal>)
        return this->AsDecimal();
    else if constexpr (std::is_same_v<T, DataTypes::JsonBinary>)
        return this->AsJson();
    else if constexpr (std::is_same_v<T, DataTypes::String>)
        return this->AsString();
    else static_assert(sizeof(T) == 0, "Value::Get<T>: unsupported type");

    return {};
}

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return a < b;
    }
};
