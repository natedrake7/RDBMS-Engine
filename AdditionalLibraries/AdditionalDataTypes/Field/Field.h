#pragma once
#include <string>
#include <vector>
#include "../../../Database/Constants.h"

using namespace std;

class Field {
    Constants::column_index_t columnIndex;
    string data;
    u16string unicodeData;
    bool isNull;
    vector<Field> children;
    Constants::Operator operatorType;
    Constants::ConditionType conditionType;

    public:
        Field();
        explicit Field(const string& data, const Constants::column_index_t& columnIndex , const bool& isNull = false);
        explicit Field(const u16string& data, const Constants::column_index_t& columnIndex , const bool& isNull = false);
        explicit Field(const string& data
                        , const Constants::column_index_t& columnIndex
                        , const Constants::Operator& operatorType
                        , const Constants::ConditionType& conditionType
                        , const bool& isNull = false);
        ~Field();
        [[nodiscard]] const string& GetData() const;
        [[nodiscard]] const u16string& GetUnicodeData() const;
        [[nodiscard]] const bool& GetIsNull() const;
        [[nodiscard]] const Constants::column_index_t& GetColumnIndex() const;
        [[nodiscard]] const Constants::ConditionType& GetConditionType() const;
        [[nodiscard]] const Constants::Operator& GetOperatorType() const;
        [[nodiscard]] const vector<Field>& GetChildren() const;
        void SetData(const string& data);
        void SetIsNull(const bool& isNull);
        void SetColumnIndex(const Constants::column_index_t& columnIndex);
        
};
