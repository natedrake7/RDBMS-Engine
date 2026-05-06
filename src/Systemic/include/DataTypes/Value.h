#pragma once
#include <string>
#include <tuple>
#include <vector>

#include "DataTypes.h"
#include "StringView.h"
#include "../Serialization/JsonParser.h"

namespace Memory{
    class IAllocator;
}

namespace DataTypes {
    class Decimal;
    class DateTime;
    class Guid;
}

class Value {
    object_t* data;
    const Memory::IAllocator* _allocator;
    block_size_t size;
    column_index_t columnIndex;
    DataType type;

    [[nodiscard]] bool TryParseAsBool()const;
    [[nodiscard]] bool TryParseAsBoolFromString()const;
    [[nodiscard]] bool TryParseAsBoolFromInt()const;
    [[nodiscard]] bool TryParseDate();

    static Value PerformBigIntAddition(const Value& lhs, const Value& rhs);
    static Value PerformStringAddition(const Value& lhs, const Value& rhs);
    static Value PerformDecimalAddition(const Value& lhs, const Value& rhs);

    static Value PerformBigIntSubtraction(const Value& lhs, const Value& rhs);
    static Value PerformDecimalSubtraction(const Value& lhs, const Value& rhs);

    [[nodiscard]] long double InterpolateString() const;

    static void BinaryOperationException(DataType lhs, DataType rhs);

    public:
        Value(const Value& copyVal);
        Value(Value&& other)noexcept;
        Value& operator=(Value&& other) noexcept;

        explicit Value(column_index_t index = 0);
        explicit Value(
            const void* data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );
        ~Value();

        Value(bool data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(TinyInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(SmallInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(Int data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(BigInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const std::string& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::String& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::StringView& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::DateTime& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::Decimal& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::Guid& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const Serialization::JsonValue& data, const Memory::IAllocator* allocator, column_index_t index = 0);

        static Value FromExternalStorage(
            const object_t* data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value Null(column_index_t columnIndex = 0);
        static Value Null(const Memory::IAllocator* allocator, column_index_t columnIndex = 0);

        [[nodiscard]] bool IsNull() const;
        [[nodiscard]] column_index_t GetColumnIndex() const;
        [[nodiscard]] DataType GetType() const;

        void SetNull();
        void SetData(bool otherData);
        void SetData(TinyInt otherData);
        void SetData(SmallInt otherData);
        void SetData(Int otherData);
        void SetData(BigInt otherData);
        void SetData(const std::string& otherData);
        void SetData(const DataTypes::Decimal& otherData);
        void SetData(const DataTypes::DateTime& otherData);
        void SetData(const DataTypes::Guid& otherData);

        void SetData(page_id_t pageId);

        [[nodiscard]] block_size_t Size() const;
        [[nodiscard]] const object_t* Data() const;
        [[nodiscard]] object_t* DataUnsafe() const;

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

        void SetColumnIndex(column_index_t otherIndex);
        void SetType(DataType otherType);
        void Deserialize(const std::vector<char>& buffer, UnsignedInt& offset);

        static inline DataType PromoteType(DataType lhs, DataType rhs);
        friend std::ostream& operator<<(std::ostream& os, const Value& field);

        Value& operator=(const Value& rhs);
        friend Value operator+(const Value& lhs, const Value& rhs);
        Value& operator+=(const Value& rhs);
        friend Value operator-(const Value& lhs, const Value& rhs);
        friend Value operator/(const Value& lhs, const Value& rhs);
        friend Value operator%(const Value& lhs, const Value& rhs);
        friend Value operator*(const Value& lhs, const Value& rhs);
        friend Value operator<(const Value& lhs, const Value& rhs);
        friend Value operator>(const Value& lhs, const Value& rhs);
        friend Value operator<=(const Value& lhs, const Value& rhs);
        friend Value operator>=(const Value& lhs, const Value& rhs);
        friend Value operator==(const Value& lhs, const Value& rhs);
        friend Value operator!=(const Value& lhs, const Value& rhs);

        [[nodiscard]] const Memory::IAllocator* GetAllocator() const;

        [[nodiscard]] bool ParseAsBoolFromString()const;
        [[nodiscard]] static Value EqualsIgnoreOrdinalCase(const Value& lhs, const Value& rhs);

        [[nodiscard]] BigInt Hash()const;
        [[nodiscard]] long double Interpolate()const;
};

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return (a < b).AsBool();
    }
};
