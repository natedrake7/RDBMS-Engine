#pragma once
#include <string>
#include "Decimal.h"

namespace DataTypes {
  class Guid;
}

static Dictionary<DataType, int> ColumnTypeRank{
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
    column_index_t columnIndex;
    object_t* data;
    block_size_t size;
    DataType type;

    [[nodiscard]] bool TryParseAsBool(bool& result)const;
    [[nodiscard]] bool TryParseAsBoolFromString(bool& result)const;
    [[nodiscard]] bool TryParseAsBoolFromInt(bool& result)const;
    [[nodiscard]] bool TryParseDate();

    static Value PerformTinyIntAddition(const int8_t& lhs, const int8_t& rhs);
    static Value PerformSmallIntAddition(const int16_t& lhs, const int16_t& rhs);
    static Value PerformIntAddition(const int32_t& lhs, const int32_t& rhs);
    static Value PerformBigIntAddition(const int64_t& lhs, const int64_t &rhs);
    static Value PerformStringAddition(const string& lhs, const string& rhs);
    static Value PerformDecimalAddition(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs);


    static Value PerformTinyIntSubtraction(const int8_t& lhs, const int8_t& rhs);
    static Value PerformSmallIntSubtraction(const int16_t& lhs, const int16_t& rhs);
    static Value PerformIntSubtraction(const int32_t& lhs, const int32_t& rhs);
    static Value PerformBigIntSubtraction(const int64_t& lhs, const int64_t &rhs);
    static Value PerformDecimalSubtraction(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs);

    static std::tuple<bool, Value> PerformNullEqualityComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullGreaterComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullGreaterEqualComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullLessComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullLessEqualComparison(const Value& lhs, const Value& rhs);
    static std::tuple<bool, Value> PerformNullInEqualityComparison(const Value& lhs, const Value& rhs);

    [[nodiscard]] long double InterpolateString() const;

    public:
        Value();
        Value(const Value& copyVal);

        //Move Constructor
        Value(Value&& other)noexcept;

        //Move Assignment Operator
        Value& operator=(Value&& other) noexcept;
        ~Value();

        explicit Value(const void* data, const column_index_t& columnIndex = 0);
        explicit Value(const void* data, const int& size, const DataType& type);
        explicit Value(const unsigned char* data, const int& size, const DataType& type);
        explicit Value(const bool& data, const column_index_t& columnIndex);
        explicit Value(const int8_t& data, const column_index_t& columnIndex);
        explicit Value(const int16_t& data, const column_index_t& columnIndex);
        explicit Value(const int32_t& data, const column_index_t& columnIndex);
        explicit Value(const int64_t& data, const column_index_t& columnIndex);
        explicit Value(const string& data, const column_index_t& columnIndex, const bool& isIdentifier = false);
        explicit Value(const u16string& data, const column_index_t& columnIndex);
        explicit Value(const DataTypes::DateTime& data, const column_index_t& columnIndex);
        explicit Value(const DataTypes::Decimal& data, const column_index_t& columnIndex);
        explicit Value(const DataTypes::Guid& data, const column_index_t& columnIndex);

        static Value Null(const column_index_t& columnIndex = 0);

        [[nodiscard]] bool IsNull() const;
        
        [[nodiscard]] const column_index_t& GetColumnIndex() const;

        [[nodiscard]] const DataType& GetType() const;
      
        void SetData(const bool& otherData);
        void SetData(const int8_t& otherData);
        void SetData(const int16_t& otherData);
        void SetData(const int32_t& otherData);
        void SetData(const int64_t& otherData);
        void SetData(const string& otherData);
        void SetData(const u16string& otherData);
        void SetData(const DataTypes::Decimal& otherData);
        void SetData(const DataTypes::DateTime& otherData);
        void SetData(const DataTypes::Guid& otherData);

        [[nodiscard]] const block_size_t& GetSize() const;

        [[nodiscard]] const object_t* GetRawData() const;
        
        [[nodiscard]] bool GetBool()const;
        [[nodiscard]] int8_t GetTinyInt()const;
        [[nodiscard]] int16_t GetSmallInt()const;
        [[nodiscard]] int32_t GetInt()const;
        [[nodiscard]] int64_t GetBigInt()const;
        [[nodiscard]] string GetString()const;
        [[nodiscard]] u16string GetUnicodeString()const;
        [[nodiscard]] DataTypes::Decimal GetDecimal()const;
        [[nodiscard]] DataTypes::DateTime GetDateTime()const;
        [[nodiscard]] time_t GetUnixTimeStamp() const;
        [[nodiscard]] DataTypes::Guid GetGuid()const;

        void SetColumnIndex(const column_index_t &otherIndex);
        void SetType(const DataType &otherType);
        void Deserialize(const std::vector<char>& buffer, uint32_t& offset);

        static DataType PromoteType(const DataType& lhs, const DataType& rhs);
        friend ostream& operator<<(ostream& os, const Value& field);

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

        [[nodiscard]] bool ParseAsBoolFromString()const;
        [[nodiscard]] static Value EqualsIgnoreOrdinalCase(const Value& lhs, const Value& rhs);

        [[nodiscard]] int64_t Hash()const;
        [[nodiscard]] long double Interpolate()const;
};

struct ValueComparator {
    bool operator()(const Value& a, const Value& b) const {
        return (a < b).GetBool();
    }
};

