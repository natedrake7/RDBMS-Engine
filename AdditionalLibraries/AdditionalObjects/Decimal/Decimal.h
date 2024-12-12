#pragma once
#include  "../../../Database/Constants.h"
#include <cmath>
#include <string>
#include <vector>

using namespace std;

class Decimal final {
    vector<byte> bytes;

    protected:
        static fraction_index_t GetFractionIndex(const string& value);
    
    public:
        Decimal();
        explicit Decimal(const string& value);
        ~Decimal();


        bool IsPositive() const;
        fraction_index_t GetFractionIndex() const;
        string ToString() const;    
};
