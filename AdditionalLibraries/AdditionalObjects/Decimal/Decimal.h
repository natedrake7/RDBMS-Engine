#pragma once
#include  "../../../Database/Constants.h"
#include <string>
#include <vector>

using namespace std;
using namespace Constants;

namespace DataTypes {
    class Decimal final {
        vector<Constants::byte> bytes;

    protected:
        static fraction_index_t GetFractionIndex(const string& value);
    
    public:
        Decimal();
        explicit Decimal(const string& value);
        explicit Decimal(const Constants::byte* data, const int& dataSize);
        ~Decimal();


        bool IsPositive() const;
        fraction_index_t GetFractionIndex() const;
        string ToString() const;

        const Constants::byte* GetRawData() const;

        int GetRawDataSize() const;
    };
}

