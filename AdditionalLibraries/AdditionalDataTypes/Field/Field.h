#pragma once
#include <string>
#include <vector>
#include "../../../Database/Constants.h"
#include "../Decimal/Decimal.h"

using namespace std;

class Field {
    column_index_t columnIndex;
    object_t* data;
    block_size_t size;
  
    bool isNotConstant;
    vector<Field> children;
    Operator operatorType;
    ConditionType conditionType;

    public:
        Field();
        explicit Field(const void* data, const column_index_t& columnIndex);
        explicit Field(const bool& data, const column_index_t& columnIndex);
        explicit Field(const int8_t& data, const column_index_t& columnIndex);
        explicit Field(const int16_t& data, const column_index_t& columnIndex);
        explicit Field(const int32_t& data, const column_index_t& columnIndex);
        explicit Field(const int64_t& data, const column_index_t& columnIndex);
        explicit Field(const string& data, const column_index_t& columnIndex);
        explicit Field(const u16string& data, const column_index_t& columnIndex);
        explicit Field(const DataTypes::DateTime& data, const column_index_t& columnIndex);
        explicit Field(const DataTypes::Decimal& data, const column_index_t& columnIndex);
        ~Field();
  
        [[nodiscard]] const string& GetData() const;
        [[nodiscard]] const u16string& GetUnicodeData() const;
        [[nodiscard]] bool GetIsNull() const;
        [[nodiscard]] const bool& GetIsNotConstant() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const ConditionType& GetConditionType() const;
        [[nodiscard]] const Operator& GetOperatorType() const;
        [[nodiscard]] const vector<Field>& GetChildren() const;
        void SetData(const bool& data);
        void SetData(const int8_t& data);
        void SetData(const int16_t& data);
        void SetData(const int32_t& data);
        void SetData(const int64_t& data);
        void SetData(const string& data);
        void SetData(const u16string& data);
        void SetData(const DataTypes::Decimal& data);
        void SetData(const DataTypes::DateTime& data);
        [[nodiscard]] bool GetBool()const;
        [[nodiscard]] int8_t GetTinyInt()const;
        [[nodiscard]] int16_t GetSmallInt()const;
        [[nodiscard]] int32_t GetInt()const;
        [[nodiscard]] int64_t GetBigInt()const;
        [[nodiscard]] char* GetString()const;
        [[nodiscard]] wchar_t* GetUnicodeString()const;
        DataTypes::Decimal GetDecimal();
        DataTypes::DateTime GetDateTime();
        void SetColumnIndex(const Constants::column_index_t &columnIndex);
};