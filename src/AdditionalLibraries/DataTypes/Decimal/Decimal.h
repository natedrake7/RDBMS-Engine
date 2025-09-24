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
        static fraction_index_t DetermineResultFractionIndex(
            const fraction_index_t& leftFractionIndex,
            const fraction_index_t& rightFractionIndex);

    
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

        [[nodiscard]] static Constants::byte CreateSignAndFractionByte(const bool& isPositive, const fraction_index_t& fractionIndex) ;

        static Decimal Add(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            const fraction_index_t& leftFractionIndex,
            const fraction_index_t& rightFractionIndex,
            const fraction_index_t& fractionIndex,
            const bool& isPositive);

        static int FractionalAdd(
                const std::vector<Constants::byte>& left,
                const std::vector<Constants::byte>& right,
                const fraction_index_t& leftFractionIndex,
                const fraction_index_t& rightFractionIndex,
                std::vector<Constants::byte>& result);

        static Decimal Subtract(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            const fraction_index_t& fractionIndex,
            const bool& isPositive);

        [[nodiscard]] static bool IsGreaterMagnitude(const std::vector<Constants::byte>& left, const std::vector<Constants::byte>& right);

        friend ostream& operator<<(ostream& os, const Decimal& decimal);
    };

    static int CompareDecimals(const vector<Constants::byte>& largerData, const int& startingIndex);

    Decimal operator+(const Decimal& left, const Decimal& right);
    bool operator==(const Decimal& left, const Decimal& right);
    bool operator>=(const Decimal& left, const Decimal& right);
    bool operator>(const Decimal& left, const Decimal& right);
    bool operator<(const Decimal& left, const Decimal& right);
    bool operator<=(const Decimal& left, const Decimal& right);
}

