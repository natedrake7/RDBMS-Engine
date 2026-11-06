#pragma once
#include <string>
#include <type_traits>

#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>
#include <Systemic/DataTypes/DateTime.h>
#include <Systemic/DataTypes/Decimal.h>
#include <Systemic/DataTypes/JsonBinary.h>
#include <Systemic/DataTypes/Guid.h>
#include <Systemic/DataTypes/LobReference.h>
#include <Systemic/Serialization/JsonParser.h>

#include "DataTypes.StaticData.h"
#include "TypeInfo.h"

namespace Memory{
    class IAllocator;
}

class Value{
    union Storage{
        bool _bool;

        Int8_t _tinyInt;
        Int16_t _smallInt;
        Int32_t _int;
        Int64_t _bigInt;

        UInt8_t


        DataTypes::DateTime _dateTime;
        DataTypes::Guid _guid;
        DataTypes::Decimal _decimal;

        const object_t* _external;
        DataTypes::LobReference _lob;
    };

    Storage _data{};

    data_size_t _size = 0;
    DataTypes::TypeInfo _typeInfo;

    column_index_t _columnIndex = 0;
    bool _isNull = true;
    bool _isLob = false;

    static_assert(std::is_trivially_destructible_v<DataTypes::Decimal>);
    static_assert(std::is_trivially_destructible_v<DataTypes::Guid>);
    static_assert(std::is_trivially_destructible_v<DataTypes::DateTime>);

    [[nodiscard]] long double InterpolateString() const;

    explicit Value(
        const object_t* data,
        data_size_t size,
        DataType type,
        const ::Memory::IAllocator* allocator,
        column_index_t index = 0
    );

    //creates a session value object
    explicit Value(
        const object_t* data,
        data_size_t size,
        DataType type,
        column_index_t index = 0
    );

    template<typename T>
    [[nodiscard]] T ReadInline()const{
        T out;
        std::memcpy(&out, &this->_data, sizeof(T));
        return out;
    }

    template<typename T>
    void WriteInline(const T& value){
        static_assert(sizeof(T) <= sizeof(Storage), "Value::WriteInline: type does not fit inline storage");

        std::memcpy(&this->_data, &value, sizeof(T));
        this->_size = sizeof(T);
        this->_typeInfo._type = DataTypes::DataTypeOf<T>();
        this->_isNull = false;
    }

    Value(const DataTypes::LobReference& data, DataType type, column_index_t index = 0);

    public:
        [[nodiscard]] static constexpr bool IsInline(const DataType type){
            return !DataTypes::IsVariableLengthType(type);
        }

        explicit Value() = default;
        Value(const Value&) = delete;
        Value& operator=(const Value&) = delete;

        Value(Value&&) = default;
        Value& operator=(Value&&) = default;

        Value(const std::string& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::StringView& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const Serialization::JsonValue& data, const Memory::IAllocator* allocator, column_index_t index = 0);

        template<DataTypes::TriviallyCopiable T>
        explicit Value(T value, const column_index_t index = 0){
            this->_columnIndex = index;
            this->WriteInline(value);
        }

        template<DataTypes::NonTriviallyCopiable T>
        Value(
            const T& other,
            const Memory::IAllocator* allocator,
            const column_index_t index = 0
        ){
            this->_columnIndex = index;
            this->_isNull = false;
            this->_typeInfo._type = DataTypes::DataTypeOf<T>();

            if constexpr (DataTypes::IsStringLike<T> || DataTypes::IsJson<T>){
                auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.Size()));
                std::memcpy(buffer, other.Data(), other.Size());
                this->_data._external = buffer;
                this->_size = other.Size();
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");
        }

        static Value FromExternalStorage(
            const object_t* data,
            data_size_t size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value SessionValue(
            const object_t* data,
            data_size_t size,
            DataType type,
            column_index_t index = 0
        );

        static Value FromLobReference(
            const DataTypes::LobReference& data,
            DataType type,
            column_index_t index = 0
        );

        static Value Null(column_index_t columnIndex = 0);

        void SetColumnIndex(const column_index_t columnIndex){
            this->_columnIndex = columnIndex;
        }

        void SetTypeInfo(const DataTypes::TypeInfo typeInfo){
            this->_typeInfo = typeInfo;
        }

        void SetNull(){
            this->_isNull = true;
            this->_size = 0;
        }

        template<DataTypes::Primitive T>
        void Set(T other){
            this->WriteInline(other);
        }

        template<DataTypes::NonPrimitiveType T>
        void Set(const T& other, const ::Memory::IAllocator* allocator){
            if constexpr (DataTypes::IsString<T>){
                auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.Size()));
                std::memcpy(buffer, other.Data(), other.Size());
                this->_data._external = buffer;
                this->_size = other.Size();
            }
            else if constexpr (DataTypes::IsJson<T>){
                auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(other.Size()));
                std::memcpy(buffer, other.Data(), other.Size());
                this->_data._external = buffer;
                this->_size = other.Size();
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");

            this->_typeInfo._type = DataTypes::DataTypeOf<T>();
            this->_isNull = false;
            this->_columnIndex = 0;
        }

        [[nodiscard]] bool IsInline() const{
            return Value::IsInline(this->_typeInfo._type);
        }

        [[nodiscard]] data_size_t Size() const{
            return this->_size;
        }

        [[nodiscard]] const object_t* Data() const{
            return this->IsInline()
                ? reinterpret_cast<const object_t*>(&this->_data)
                : this->_data._external;
        }

        template<DataTypes::Primitive T>
        [[nodiscard]] T Data() const{
            return this->ReadInline<T>();
        }

        [[nodiscard]] bool IsNull() const{
            return this->_isNull;
        }

        [[nodiscard]] column_index_t GetColumnIndex() const{
            return this->_columnIndex;
        }

        [[nodiscard]] DataTypes::TypeInfo TypeInfo() const{
            return this->_typeInfo;
        }

        [[nodiscard]] bool IsLob() const{
            return this->_isLob;
        }

        [[nodiscard]] bool AsBool()const;
        [[nodiscard]] Int8_t AsTinyInt()const;
        [[nodiscard]] Int16_t AsSmallInt()const;
        [[nodiscard]] Int32_t AsInt()const;
        [[nodiscard]] Int64_t AsBigInt()const;
        [[nodiscard]] DataTypes::String AsString(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] std::string AsStdString()const;
        [[nodiscard]] DataTypes::StringView AsStringView()const;
        [[nodiscard]] DataTypes::Decimal AsDecimal()const;
        [[nodiscard]] DataTypes::DateTime AsDateTime()const;
        [[nodiscard]] time_t AsUnixTimeStamp() const;
        [[nodiscard]] DataTypes::Guid AsGuid()const;
        [[nodiscard]] DataTypes::JsonBinary AsJson(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] DataTypes::LobReference AsLobReference() const;

        [[nodiscard]] bool IsIntegral()const{
            return this->_typeInfo._type >= DataType::TinyInt
                && this->_typeInfo._type <= DataType::BigInt;
        }
        [[nodiscard]] bool ParseAsBoolFromString()const;

        [[nodiscard]] long double Interpolate()const;

        template<typename T>
        [[nodiscard]] T Get(const ::Memory::IAllocator* allocator) const;

        friend bool operator<(const Value& lhs, const Value& rhs);
        friend bool operator==(const Value& lhs, const Value& rhs);
        friend bool operator>(const Value& lhs, const Value& rhs);
        friend bool operator<=(const Value& lhs, const Value& rhs);
        friend bool operator>=(const Value& lhs, const Value& rhs);
        friend bool operator!=(const Value& lhs, const Value& rhs);
};

// Typed unbox: maps a compile-time T to the matching runtime accessor. The
// discarded `if constexpr` branches are never instantiated, so forward-declared
// return types (Decimal/DateTime/Guid) only need to be complete in the TU that
// actually instantiates that branch.
template <typename T>
T Value::Get(const ::Memory::IAllocator* allocator) const{
    if constexpr (std::is_same_v<T, bool>)
        return this->AsBool();
    else if constexpr (std::is_same_v<T, Int8_t>)
        return this->AsTinyInt();
    else if constexpr (std::is_same_v<T, Int16_t>)
        return this->AsSmallInt();
    else if constexpr (std::is_same_v<T, Int32_t>)
        return this->AsInt();
    else if constexpr (std::is_same_v<T, Int64_t>)
        return this->AsBigInt();
    else if constexpr (std::is_same_v<T, DataTypes::DateTime>)
        return this->AsDateTime();
    else if constexpr (std::is_same_v<T, DataTypes::Guid>)
        return this->AsGuid();
    else if constexpr (std::is_same_v<T, DataTypes::Decimal>)
        return this->AsDecimal();
    else if constexpr (std::is_same_v<T, DataTypes::JsonBinary>)
        return this->AsJson(allocator);
    else if constexpr (std::is_same_v<T, DataTypes::String>)
        return this->AsString(allocator);
    else static_assert(sizeof(T) == 0, "Value::Get<T>: unsupported type");

    std::unreachable();
}

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return a < b;
    }
};
