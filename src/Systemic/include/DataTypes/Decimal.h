#pragma once

#include <string>
#include <vector>

#include "String.h"
#include "../DataStructures/StaticArray.h"

static constexpr Int DECIMAL_ARRAY_SIZE = 20;
static constexpr Int DECIMAL_TEMPORARY_BUFFER_SIZE = 2 * DECIMAL_ARRAY_SIZE;
static constexpr Int DECIMAL_MULTIPLICATION_BUFFER_SIZE = 4 * DECIMAL_ARRAY_SIZE;

static constexpr Int DECIMAL_ZERO = 0x00;
static constexpr Int DECIMAL_HEADER_INDEX = 0;
static constexpr Int DECIMAL_DIGITS_START_INDEX = 1;

namespace DataTypes {
    class StringView;

    class Decimal final {
        DataStructures::StaticArray<byte_t, DECIMAL_ARRAY_SIZE> _data;

        using DataBuffer = DataStructures::StaticArray<byte_t, DECIMAL_ARRAY_SIZE>;
        using TempBuffer = DataStructures::StaticArray<Int, DECIMAL_TEMPORARY_BUFFER_SIZE>;
        using MultiplicationBuffer = DataStructures::StaticArray<Int, DECIMAL_MULTIPLICATION_BUFFER_SIZE>;

        enum class ComparisonResult : TinyInt{
            Less = -1,
            Equal = 0,
            Greater = 1,
        };

    protected:
        template <typename T>
        void InitializeFromInteger(T value);

        static TempBuffer Unpack(const DataBuffer& bytes);
        static MultiplicationBuffer MultiplyDigits(
            const TempBuffer& leftDigits,
            const TempBuffer& rightDigits
        );
        static DataBuffer Pack(
            const TempBuffer& digits,
            bool isPositive,
            fraction_index_t fractionIndex
        );

        static DataBuffer Pack(
            const MultiplicationBuffer& digits,
            bool isPositive,
            fraction_index_t fractionIndex
        );

        // static fraction_index_t GetFractionIndex(const StringView& value);

        static fraction_index_t DetermineResultFractionIndex(
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );
        static ComparisonResult CompareDecimalsWithoutSign(
            const DataBuffer& leftData,
            const DataBuffer& rightData
        );

        static Decimal Add(
            const DataBuffer& left,
            const DataBuffer& right,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static void PadFractionalParts(
            DataBuffer& left, DataBuffer& right,
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );

        static void PadNonFractionalParts(
            DataBuffer& left, DataBuffer& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static Decimal Subtract(
            const DataBuffer& left, const DataBuffer& right,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static Decimal Multiply(
            const DataBuffer& left, const DataBuffer& right,
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
            MultiplicationBuffer& digits,
            fraction_index_t& fractionIndex
        );

        static void TrimTrailingZeros(
            MultiplicationBuffer& digits,
            fraction_index_t fractionIndex
        );

        static void PadDecimalParts(
            MultiplicationBuffer& digits,
            fraction_index_t& fractionIndex
        );

        [[nodiscard]] static byte_t CreateSignAndFractionByte(bool isPositive, fraction_index_t fractionIndex) ;

        [[nodiscard]] static bool IsGreaterMagnitude(const DataBuffer& left, const DataBuffer& right);

    public:
        Decimal();
        explicit Decimal(const StringView& value);
        explicit Decimal(const byte_t* data, Int dataSize);
        // explicit Decimal(const std::vector<byte_t>& value);
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

        [[nodiscard]] const DataBuffer& Data() const;

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

