#pragma once
#include  "../../../Database/Constants.h"
#include <string>
#include <vector>
#include <limits>

using namespace std;
using namespace Constants;

namespace DataTypes {
    class Decimal final {
        vector<Constants::byte> bytes;

    protected:
        template <typename T>
        void InitializeFromInteger(const T& value);

        static std::vector<int> Unpack(const vector<Constants::byte>& bytes);
        static std::vector<int> MultiplyDigits(const std::vector<int>& leftDigits, const std::vector<int>& rightDigits);
        static std::vector<Constants::byte> Pack(
            const std::vector<int>& digits,
            const bool& isPositive,
            const fraction_index_t& fractionIndex
        );

        static fraction_index_t GetFractionIndex(const string& value);

        static fraction_index_t DetermineResultFractionIndex(
            const fraction_index_t& leftFractionIndex,
            const fraction_index_t& rightFractionIndex
        );
        static int CompareDecimalsWithoutSign(
            const std::vector<Constants::byte>& leftData,
            const std::vector<Constants::byte>& rightData
        );

        static Decimal Add(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            const fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static void PadFractionalParts(
            std::vector<Constants::byte>& left,
            std::vector<Constants::byte>& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static void PadNonFractionalParts(
            std::vector<Constants::byte>& left,
            std::vector<Constants::byte>& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static Decimal Subtract(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            const fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static Decimal Multiply(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static Decimal Divide(
            const std::vector<Constants::byte>& left,
            const std::vector<Constants::byte>& right,
            fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static void TrimLeadingZeros(
            std::vector<int>& digits,
            fraction_index_t& fractionIndex
        );

        static void TrimTrailingZeros(
            std::vector<int>& digits,
            const fraction_index_t& fractionIndex
        );

        static void PadDecimalParts(
            std::vector<int>& digits,
            fraction_index_t& fractionIndex
        );

    public:
        Decimal();
        explicit Decimal(const string& value);
        explicit Decimal(const Constants::byte* data, const int& dataSize);
        explicit Decimal(const vector<Constants::byte>& value);
        explicit Decimal(const bool& value);
        explicit Decimal(const int8_t& value);
        explicit Decimal(const int16_t& value);
        explicit Decimal(const int32_t& value);
        explicit Decimal(const int64_t& value);
        ~Decimal();


        [[nodiscard]] bool IsPositive() const;
        [[nodiscard]] fraction_index_t GetFractionIndex() const;
        [[nodiscard]] string ToString() const;

        [[nodiscard]] const Constants::byte* GetRawData() const;

        [[nodiscard]] int GetRawDataSize() const;

        [[nodiscard]] const vector<Constants::byte>& GetData() const;

        [[nodiscard]] static Constants::byte CreateSignAndFractionByte(const bool& isPositive, const fraction_index_t& fractionIndex) ;

        [[nodiscard]] static bool IsGreaterMagnitude(const std::vector<Constants::byte>& left, const std::vector<Constants::byte>& right);

        friend ostream& operator<<(ostream& os, const Decimal& decimal);

        friend Decimal operator+(const Decimal& left, const Decimal& right);
        friend Decimal operator-(const Decimal& left, const Decimal& right);
        friend Decimal operator*(const Decimal& left, const Decimal& right);
        friend Decimal operator/(const Decimal& left, const Decimal& right);

        friend Decimal operator+(const Decimal& left, const int64_t& right);
        friend Decimal operator-(const Decimal& left, const int64_t& right);
        friend Decimal operator*(const Decimal& left, const int64_t& right);
        friend Decimal operator/(const Decimal& left, const int64_t& right);

        friend bool operator==(const Decimal& left, const Decimal& right);
        friend bool operator>=(const Decimal& left, const Decimal& right);
        friend bool operator>(const Decimal& left, const Decimal& right);
        friend bool operator<(const Decimal& left, const Decimal& right);
        friend bool operator<=(const Decimal& left, const Decimal& right);
    };



// Specialization
}

template<> struct std::numeric_limits<DataTypes::Decimal> {
    static constexpr bool is_specialized = true;

    static DataTypes::Decimal min() noexcept {
        static DataTypes::Decimal value("-9999999999999999.9999");
        return value;
    }

    static DataTypes::Decimal max() noexcept {
        static DataTypes::Decimal value("9999999999999999.9999");
        return value;
    }

    static constexpr int digits10 = 34;  // max base-10 precision
    static constexpr int radix = 10;     // base of your decimal
};

