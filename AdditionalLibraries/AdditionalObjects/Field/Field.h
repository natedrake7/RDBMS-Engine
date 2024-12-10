#pragma once
#include <string>

#include "../../../Database/Constants.h"

using namespace std;

class Field {
    column_index_t columnIndex;
    string data;
    bool isNull;

    public:
        Field();
        explicit Field(const string& data,const column_index_t& columnIndex ,const bool& isNull = false);
        ~Field();
        const string& GetData() const;
        const bool& GetIsNull() const;
        const column_index_t& GetColumnIndex() const;
        void SetData(const string& data);
        void SetIsNull(const bool& isNull);
        void SetColumnIndex(const column_index_t& columnIndex);
        
};
