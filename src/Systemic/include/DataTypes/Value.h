#pragma once
#include <cstring>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include "DataTypes.h"

namespace Memory{
    class IAllocator;
}

namespace DataTypes {
    class Decimal;
    class DateTime;
    class Guid;
}

inline Dictionary<DataType, int> ColumnTypeRank{
  {DataType::String, 1},
  {DataType::UnicodeString, 2},
  {DataType::Bool, 3},
  {DataType::TinyInt, 4},
  {DataType::SmallInt, 5},
  {DataType::Int, 6},
  {DataType::BigInt, 7},
  {DataType::Decimal, 8},
  {DataType::DateTime, 9},
};

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

    // static Value PerformTinyIntAddition(TinyInt lhs, TinyInt rhs);
    // static Value PerformSmallIntAddition(SmallInt lhs, SmallInt rhs);
    // static Value PerformIntAddition(Int lhs, Int rhs);
    static Value PerformBigIntAddition(const Value& lhs, const Value& rhs);
    static Value PerformStringAddition(const Value& lhs, const Value& rhs);
    static Value PerformDecimalAddition(const Value& lhs, const Value& rhs);


    // static Value PerformTinyIntSubtraction(TinyInt lhs, TinyInt rhs);
    // static Value PerformSmallIntSubtraction(SmallInt lhs, SmallInt rhs);
    // static Value PerformIntSubtraction(Int lhs, Int rhs);
    static Value PerformBigIntSubtraction(const Value& lhs, const Value& rhs);
    static Value PerformDecimalSubtraction(const Value& lhs, const Value& rhs);

    static std::tuple<bool, Value> PerformNullEqualityComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullGreaterComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullGreaterEqualComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullLessComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullLessEqualComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullInEqualityComparison(const Value& lhs, const Value& rhs);

    [[nodiscard]] long double InterpolateString() const;

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
        // explicit Value(bool data, column_index_t index = 0);
        // explicit Value(TinyInt data, column_index_t index = 0);
        // explicit Value(SmallInt data, column_index_t index = 0);
        // explicit Value(Int data, column_index_t index = 0);
        // explicit Value(BigInt data, column_index_t index = 0);
        // explicit Value(const std::string& data, column_index_t index = 0);
        // explicit Value(const DataTypes::DateTime& data, column_index_t index = 0);
        // explicit Value(const DataTypes::Decimal& data, column_index_t index = 0);
        // explicit Value(const DataTypes::Guid& data, column_index_t index = 0);

        Value(bool data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(TinyInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(SmallInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(Int data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(BigInt data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const std::string& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::DateTime& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::Decimal& data, const Memory::IAllocator* allocator, column_index_t index = 0);
        Value(const DataTypes::Guid& data, const Memory::IAllocator* allocator, column_index_t index = 0);

        static Value FromExternalStorage(
            const object_t* data,
            Int size,
            DataType type,
            const Memory::IAllocator* allocator,
            column_index_t index = 0
        );

        static Value Null(column_index_t columnIndex = 0);

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
        [[nodiscard]] std::string AsString()const;
        [[nodiscard]] std::u16string AsUnicodeString()const;
        [[nodiscard]] DataTypes::Decimal AsDecimal()const;
        [[nodiscard]] DataTypes::DateTime AsDateTime()const;
        [[nodiscard]] time_t AsUnixTimeStamp() const;
        [[nodiscard]] DataTypes::Guid AsGuid()const;
        [[nodiscard]] page_id_t AsLargeObjectPointer() const;
        // [[nodiscard]] Pages::OverflowPointer AsOverflowPointer() const;

        void SetColumnIndex(column_index_t otherIndex);
        void SetType(DataType otherType);
        void Deserialize(const std::vector<char>& buffer, UnsignedInt& offset);

        static DataType PromoteType(DataType lhs, DataType rhs);
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

        void Resize(Int newSize);
};

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return (a < b).AsBool();
    }
};
