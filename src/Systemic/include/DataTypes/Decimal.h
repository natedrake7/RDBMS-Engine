#pragma once
#include <array>

#include "../../../DatabaseEngine/include/DatabaseConstants.h"
#include <string>
#include <vector>
#include <limits>

#include "String.h"

using namespace Constants;

static constexpr Int DECIMAL_ARRAY_SIZE = 20;

namespace DataTypes {
    class StringView;

    class Decimal final {
        std::array<byte_t, DECIMAL_ARRAY_SIZE> _data;
        Int _size;

    protected:
        template <typename T>
        void InitializeFromInteger(T value);

        static std::vector<Int> Unpack(const std::vector<byte_t>& bytes);
        static std::vector<Int> MultiplyDigits(const std::vector<Int>& leftDigits, const std::vector<Int>& rightDigits);
        static std::array<byte_t, DECIMAL_ARRAY_SIZE> Pack(
            const std::array<byte_t, 2 * DECIMAL_ARRAY_SIZE> &digits,
            bool isPositive,
            fraction_index_t fractionIndex
        );

        static fraction_index_t GetFractionIndex(const std::string& value);

        static fraction_index_t DetermineResultFractionIndex(
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );
        static int CompareDecimalsWithoutSign(
            const std::vector<byte_t>& leftData,
            const std::vector<byte_t>& rightData
        );

        static Decimal Add(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            fraction_index_t fractionIndex,
            bool isPositive
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
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static Decimal Multiply(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            fraction_index_t& fractionIndex,
            bool isPositive
        );

        static Decimal Divide(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            fraction_index_t& fractionIndex,
            bool isPositive
        );

        static void TrimLeadingZeros(
            std::vector<Int>& digits,
            fraction_index_t& fractionIndex
        );

        static void TrimTrailingZeros(
            std::vector<Int>& digits,
            fraction_index_t fractionIndex
        );

        static void PadDecimalParts(
            std::vector<Int>& digits,
            fraction_index_t& fractionIndex
        );

        [[nodiscard]] static byte_t CreateSignAndFractionByte(bool isPositive, fraction_index_t fractionIndex) ;

        [[nodiscard]] static bool IsGreaterMagnitude(const std::vector<byte_t>& left, const std::vector<byte_t>& right);

    public:
        Decimal();
        explicit Decimal(const StringView& value);
        explicit Decimal(const byte_t* data, Int dataSize);
        explicit Decimal(const std::vector<byte_t>& value);
        explicit Decimal(bool value);
        explicit Decimal(TinyInt value);
        explicit Decimal(SmallInt value);
        explicit Decimal(Int value);
        explicit Decimal(BigInt value);
        ~Decimal();


        [[nodiscard]] bool IsPositive() const;
        [[nodiscard]] fraction_index_t GetFractionIndex() const;
        [[nodiscard]] String ToString(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] const byte_t* GetRawData() const;

        [[nodiscard]] Int GetRawDataSize() const;

        [[nodiscard]] const std::vector<byte_t>& GetData() const;

        [[nodiscard]] static Int Size(Int precision);
        [[nodiscard]] double ToDouble() const;

        friend std::ostream& operator<<(std::ostream& os, const Decimal& decimal);

        friend Decimal operator+(const Decimal& left, const Decimal& right);
        friend Decimal operator-(const Decimal& left, const Decimal& right);
        friend Decimal operator*(const Decimal& left, const Decimal& right);
        friend Decimal operator/(const Decimal& left, const Decimal& right);

        friend Decimal operator+(const Decimal& left, BigInt right);
        friend Decimal operator-(const Decimal& left, BigInt right);
        friend Decimal operator*(const Decimal& left, BigInt right);
        friend Decimal operator/(const Decimal& left, BigInt right);

        friend bool operator==(const Decimal& left, const Decimal& right);
        friend bool operator>=(const Decimal& left, const Decimal& right);
        friend bool operator>(const Decimal& left, const Decimal& right);
        friend bool operator<(const Decimal& left, const Decimal& right);
        friend bool operator<=(const Decimal& left, const Decimal& right);

        Decimal& operator=(TinyInt right);
        Decimal& operator=(SmallInt right);
        Decimal& operator=(Int right);
        Decimal& operator=(BigInt right);

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

