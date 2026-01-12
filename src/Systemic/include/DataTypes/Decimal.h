#pragma once
#include "../../../DatabaseEngine/include/DatabaseConstants.h"
#include <string>
#include <vector>
#include <limits>

using namespace std;
using namespace Constants;

namespace DataTypes {
    class Decimal final {
        vector<byte_t> bytes;

    protected:
        template <typename T>
        void InitializeFromInteger(const T& value);

        static std::vector<int> Unpack(const vector<byte_t>& bytes);
        static std::vector<int> MultiplyDigits(const std::vector<int>& leftDigits, const std::vector<int>& rightDigits);
        static std::vector<byte_t> Pack(
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
            const std::vector<byte_t>& leftData,
            const std::vector<byte_t>& rightData
        );

        static Decimal Add(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            const fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static void PadFractionalParts(
            std::vector<byte_t>& left,
            std::vector<byte_t>& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static void PadNonFractionalParts(
            std::vector<byte_t>& left,
            std::vector<byte_t>& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static Decimal Subtract(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            const fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static Decimal Multiply(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            fraction_index_t& fractionIndex,
            const bool& isPositive
        );

        static Decimal Divide(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
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

        [[nodiscard]] static byte_t CreateSignAndFractionByte(const bool& isPositive, const fraction_index_t& fractionIndex) ;

        [[nodiscard]] static bool IsGreaterMagnitude(const std::vector<byte_t>& left, const std::vector<byte_t>& right);

    public:
        Decimal();
        explicit Decimal(const string& value);
        explicit Decimal(const byte_t* data, const int& dataSize);
        explicit Decimal(const vector<byte_t>& value);
        explicit Decimal(const bool& value);
        explicit Decimal(const int8_t& value);
        explicit Decimal(const int16_t& value);
        explicit Decimal(const int32_t& value);
        explicit Decimal(const int64_t& value);
        ~Decimal();


        [[nodiscard]] bool IsPositive() const;
        [[nodiscard]] fraction_index_t GetFractionIndex() const;
        [[nodiscard]] string ToString() const;

        [[nodiscard]] const byte_t* GetRawData() const;

        [[nodiscard]] int GetRawDataSize() const;

        [[nodiscard]] const vector<byte_t>& GetData() const;

        [[nodiscard]] static int Size(const int& precision);
        [[nodiscard]] double ToDouble() const;

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

        Decimal& operator=(const int8_t& right);
        Decimal& operator=(const int16_t& right);
        Decimal& operator=(const int32_t& right);
        Decimal& operator=(const int64_t& right);

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

