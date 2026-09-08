#pragma once
#include <string>
#include <type_traits>

#include "DataTypes.h"
#include "StringView.h"
#include "DateTime.h"
#include "Decimal.h"
#include "JsonBinary.h"
#include "Guid.h"
#include "../../../Server/include/ConnectionManager.h"
#include "../Serialization/JsonParser.h"

namespace Memory{
    class IAllocator;
}

class Value{
    union Storage{
        bool _bool;
        TinyInt _tinyInt;
        SmallInt _smallInt;
        Int _int;
        BigInt _bigInt;
        DataTypes::DateTime _dateTime;
        DataTypes::Guid _guid;
        DataTypes::Decimal _decimal;

        const object_t* _external;
    };

    Storage _data{};

    block_size_t _size = 0;
    column_index_t _columnIndex = 0;
    DataType _type = DataType::Null;
    bool _isNull = true;

    static_assert(std::is_trivially_destructible_v<DataTypes::Decimal>);
    static_assert(std::is_trivially_destructible_v<DataTypes::Guid>);
    static_assert(std::is_trivially_destructible_v<DataTypes::DateTime>);

    [[nodiscard]] long double InterpolateString() const;

    explicit Value(
        const object_t* data,
        Int size,
        DataType type,
        const ::Memory::IAllocator* allocator,
        column_index_t index = 0
    );

    //creates a session value object
    explicit Value(
        const object_t* data,
        Int size,
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
        std::memcpy(&this->_data, &value, sizeof(T));
        this->_size = sizeof(T);
        this->_type = DataTypes::DataTypeOf<T>();
        this->_isNull = false;
    }

    public:
        [[nodiscard]] static constexpr bool IsInline(const DataType type){
            return type != DataType::String && type != DataType::Json;
        }

        Value(column_index_t index = 0);

        Value(const std::string& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::StringView& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const Serialization::JsonValue& data, const Memory::IAllocator* allocator, column_index_t index = 0);

        template<DataTypes::TriviallyCopiable T>
        explicit Value(T other, const column_index_t index = 0){
            this->_columnIndex = index;
            this->WriteInline(other);
        }

        template<DataTypes::NonTriviallyCopiable T>
        Value(
            const T& other,
            const Memory::IAllocator* allocator,
            const column_index_t index = 0
        ){
            this->_columnIndex = index;
            this->_isNull = false;
            this->_type = DataTypes::DataTypeOf<T>();

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
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value SessionValue(
            const object_t* data,
            Int size,
            DataType type,
            column_index_t index = 0
        );

        static Value Null(column_index_t columnIndex = 0);

        void SetColumnIndex(column_index_t otherIndex);
        void SetType(DataType otherType);

        void SetNull();

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
            else if constexpr (DataTypes::IsDecimal<T>){
                std::memcpy(&this->_data, &other, sizeof(DataTypes::Decimal));
                this->_size = sizeof(DataTypes::Decimal);
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "Value::Value<T>: unsupported type");

            this->_type = DataTypes::DataTypeOf<T>();
            this->_isNull = false;
            this->_columnIndex = 0;
        }

        [[nodiscard]] bool IsInline() const;
        [[nodiscard]] block_size_t Size() const;
        [[nodiscard]] const object_t* Data() const;


        template<DataTypes::Primitive T>
        [[nodiscard]] T Data() const{
            return *reinterpret_cast<const T*>(&this->_data);
        }

        template<DataTypes::IsDecimal T>
        [[nodiscard]] T Data()const{
            return *reinterpret_cast<const T*>(&this->_data);
        }

        [[nodiscard]] bool IsNull() const;
        [[nodiscard]] column_index_t GetColumnIndex() const;
        [[nodiscard]] DataType GetType() const;

        [[nodiscard]] bool AsBool()const;
        [[nodiscard]] TinyInt AsTinyInt()const;
        [[nodiscard]] SmallInt AsSmallInt()const;
        [[nodiscard]] Int AsInt()const;
        [[nodiscard]] BigInt AsBigInt()const;
        [[nodiscard]] DataTypes::String AsString(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] std::string AsStdString()const;
        [[nodiscard]] DataTypes::StringView AsStringView()const;
        [[nodiscard]] DataTypes::Decimal AsDecimal()const;
        [[nodiscard]] DataTypes::DateTime AsDateTime()const;
        [[nodiscard]] time_t AsUnixTimeStamp() const;
        [[nodiscard]] DataTypes::Guid AsGuid()const;
        [[nodiscard]] DataTypes::JsonBinary AsJson(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] page_id_t AsLargeObjectPointer() const;

        [[nodiscard]] bool IsIntegral()const;
        // friend std::ostream& operator<<(std::ostream& os, const Value& field);

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
        return this->AsJson(allocator);
    else if constexpr (std::is_same_v<T, DataTypes::String>)
        return this->AsString(allocator);
    else static_assert(sizeof(T) == 0, "Value::Get<T>: unsupported type");

    return {};
}

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return a < b;
    }
};
