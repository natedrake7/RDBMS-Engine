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
        explicit Decimal(const vector<Constants::byte>& value);
        ~Decimal();


        [[nodiscard]] bool IsPositive() const;
        [[nodiscard]] fraction_index_t GetFractionIndex() const;
        [[nodiscard]] string ToString() const;

        [[nodiscard]] const Constants::byte* GetRawData() const;

        [[nodiscard]] int GetRawDataSize() const;

        [[nodiscard]] const vector<Constants::byte>& GetData() const;
    };

    Decimal operator+(const Decimal& left, const Decimal& right);
    bool operator==(const Decimal& left, const Decimal& right);
}

