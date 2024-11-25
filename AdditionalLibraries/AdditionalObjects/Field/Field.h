#pragma once
#include <string>

using namespace std;

class Field {
    string data;
    bool isNull;

    public:
        Field();
        Field(const string& data, const bool& isNull = false);
        ~Field();
        const string& GetData() const;
        const bool& GetIsNull() const;
        void SetData(const string& data);
        void SetIsNull(const bool& isNull);
};
