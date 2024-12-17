#pragma once
#include  "../../../Database/Constants.h"
#include <cmath>
#include <string>
#include <vector>

using namespace std;

class Decimal final {
    vector<Constants::byte> bytes;

    protected:
        static Constants::fraction_index_t GetFractionIndex(const string& value);
    
    public:
        Decimal();
        explicit Decimal(const string& value);
        explicit Decimal(const Constants::byte* data, const int& dataSize);
        ~Decimal();


        bool IsPositive() const;
        Constants::fraction_index_t GetFractionIndex() const;
        string ToString() const;

        const Constants::byte* GetRawData() const;

        int GetRawDataSize() const;
};
