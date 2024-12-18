#pragma once
#include <string>

#include "../../../Database/Constants.h"

using namespace std;

class Field {
    Constants::column_index_t columnIndex;
    string data;
    bool isNull;

    public:
        Field();
        explicit Field(const string& data,const Constants::column_index_t& columnIndex ,const bool& isNull = false);
        ~Field();
        [[nodiscard]] const string& GetData() const;
        [[nodiscard]] const bool& GetIsNull() const;
        [[nodiscard]] const Constants::column_index_t& GetColumnIndex() const;
        void SetData(const string& data);
        void SetIsNull(const bool& isNull);
        void SetColumnIndex(const Constants::column_index_t& columnIndex);
        
};
