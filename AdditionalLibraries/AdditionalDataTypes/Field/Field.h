#pragma once
#include <string>
#include "../../../Database/Constants.h"
#include "../Decimal/Decimal.h"
#include "../Guid/Guid.h"
#include "../Headers/Headers.h"

using namespace std;

class Field {
    column_index_t columnIndex;
    object_t* data;
    block_size_t size;
    ColumnType type;

    bool isIdentifier;
    std::string name;

    [[nodiscard]] bool TryParseAsBool(bool& result)const;
    [[nodiscard]] bool TryParseAsBoolFromString(bool& result)const;
    [[nodiscard]] bool ParseAsBoolFromString()const;
    [[nodiscard]] bool TryParseAsBoolFromInt(bool& result)const;
    [[nodiscard]] bool TryParseDate();

    public:
        Field();
        ~Field();
        
        explicit Field(const void* data, const column_index_t& columnIndex = 0);
        
        explicit Field(const bool& data, const column_index_t& columnIndex);
        
        explicit Field(const int8_t& data, const column_index_t& columnIndex);
        
        explicit Field(const int16_t& data, const column_index_t& columnIndex);
        
        explicit Field(const int32_t& data, const column_index_t& columnIndex);
        
        explicit Field(const int64_t& data, const column_index_t& columnIndex);
        
        explicit Field(const string& data, const column_index_t& columnIndex, const bool& isIdentifier = false);
        
        explicit Field(const u16string& data, const column_index_t& columnIndex);
        
        explicit Field(const DataTypes::DateTime& data, const column_index_t& columnIndex);
        
        explicit Field(const DataTypes::Decimal& data, const column_index_t& columnIndex);

        explicit Field(const DataTypes::Guid& data, const column_index_t& columnIndex);

        [[nodiscard]] bool GetIsNull() const;
        
        [[nodiscard]] const column_index_t& GetColumnIndex() const;

        [[nodiscard]] const ColumnType& GetType() const;
      
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

        void SetName(std::string& data);

        void InferType();

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

        void SetColumnIndex(const Constants::column_index_t &columnIndex);

        void SetType(const ColumnType &type);

        void Validate(const Headers::ColumnHeader &header);

        void Validate(const ColumnType& columnType, const int& ordinalPosition);

        friend ostream& operator<<(ostream& os, const Field& field);

        [[nodiscard]] bool IsVariable()const;
};
