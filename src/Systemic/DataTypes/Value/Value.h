#pragma once
#include <string>
#include "../../../Database/Constants.h"
#include "../Decimal/Decimal.h"
#include "../Guid/Guid.h"

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
    Constants::column_index_t columnIndex;
    Constants::object_t* data;
    Constants::block_size_t size;
    Constants::DataType type;

    bool isIdentifier;
    std::string name;

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
    static std::tuple<bool, Value> PerformNullInEqualityComparison(const Value& lhs, const Value& rhs);

    public:
        Value();
        Value(const Value& copyVal);

        //Move Constructor
        Value(Value&& other)noexcept;

        //Move Assignment Operator
        Value& operator=(Value&& other) noexcept;
        ~Value();

        explicit Value(const void* data, const Constants::column_index_t& columnIndex = 0);

        explicit Value(const void* data, const int& size, const Constants::DataType& type);

        explicit Value(const unsigned char* data, const int& size, const Constants::DataType& type);
        
        explicit Value(const bool& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const int8_t& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const int16_t& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const int32_t& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const int64_t& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const string& data, const Constants::column_index_t& columnIndex, const bool& isIdentifier = false);
        
        explicit Value(const u16string& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const DataTypes::DateTime& data, const Constants::column_index_t& columnIndex);
        
        explicit Value(const DataTypes::Decimal& data, const Constants::column_index_t& columnIndex);

        explicit Value(const DataTypes::Guid& data, const Constants::column_index_t& columnIndex);

        [[nodiscard]] bool GetIsNull() const;
        
        [[nodiscard]] const Constants::column_index_t& GetColumnIndex() const;

        [[nodiscard]] const Constants::DataType& GetType() const;
      
        void SetData(const bool& data);
      
        void SetData(const int8_t& data);
      
        void SetData(const int16_t& data);
      
        void SetData(const int32_t& data);
      
        void SetData(const int64_t& data);
      
        void SetData(const string& data);
      
        void SetData(const u16string& data);
      
        void SetData(const DataTypes::Decimal& data);
      
        void SetData(const DataTypes::DateTime& data);

        void SetData(const DataTypes::Guid& data);

        void SetName(std::string& name);

        [[nodiscard]] const Constants::block_size_t& GetSize() const;

        [[nodiscard]] const Constants::object_t* GetRawData() const;
        
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

        void SetColumnIndex(const Constants::column_index_t &columnIndex);

        void SetType(const Constants::DataType &type);

        void Deserialize(const std::vector<char>& buffer, uint32_t& offset);

        static Constants::DataType PromoteType(const Constants::DataType& lhs, const Constants::DataType& rhs);

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

        [[nodiscard]] bool IsVariable()const;
        [[nodiscard]] bool ParseAsBoolFromString()const;
        [[nodiscard]] static Value EqualsIgnoreOrdinalCase(const Value& lhs, const Value& rhs);
};
