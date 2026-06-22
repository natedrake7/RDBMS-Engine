#pragma once

#include "../Comparators.h"
#include "String.h"
#include "../DataStructures/StaticArray.h"
#include "../Functions/MathFunctions.h"
#include "DataTypes.h"


namespace DataTypes {
    class StringView;

    enum class DecimalRoundingMode : UnsignedTinyInt{
        Truncate = 0,
        HalfUp = 1,
        HalfEven = 2
    };

    static constexpr Int DECIMAL_ARRAY_SIZE = 20;
    static constexpr Int DECIMAL_MAX_NUMBER_OF_DIGITS = (DECIMAL_ARRAY_SIZE - 1) * 2;
    static constexpr Int DECIMAL_TEMPORARY_BUFFER_SIZE = 2 * DECIMAL_ARRAY_SIZE;
    static constexpr Int DECIMAL_MULTIPLICATION_BUFFER_SIZE = 4 * DECIMAL_ARRAY_SIZE;

    static constexpr Int DECIMAL_ZERO = 0x00;
    static constexpr Int DECIMAL_HEADER_INDEX = 0;
    static constexpr Int DECIMAL_DIGITS_START_INDEX = 1;

    using DataBuffer = DataStructures::StaticArray<byte_t, DECIMAL_ARRAY_SIZE>;
    using StringBuffer = DataStructures::StaticArray<char, 2 * DECIMAL_ARRAY_SIZE>;
    using DigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_TEMPORARY_BUFFER_SIZE>;
    using MultiplicationDigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_MULTIPLICATION_BUFFER_SIZE>;

    template<typename T>
    concept IsDecimalBuffer = std::is_same_v<T, DataBuffer>
            || std::is_same_v<T, DigitsBuffer>
            || std::is_same_v<T, MultiplicationDigitsBuffer>;

    class Decimal final {
        DataStructures::StaticArray<byte_t, DECIMAL_ARRAY_SIZE> _data;

        static constexpr Int LowNibble(const DataBuffer& buffer, Int index);
        static constexpr Int HighNibble(const DataBuffer& buffer, Int index);

        static constexpr byte_t PackNibbles(Int highNibble, Int lowNibble);

        static constexpr DigitsBuffer Unpack(const DataBuffer& bytes);

        static constexpr DigitsBuffer ToBase100(const DataBuffer& bytes);
        static constexpr MultiplicationDigitsBuffer Base100ToDigits(const MultiplicationDigitsBuffer& input);
        static constexpr MultiplicationDigitsBuffer MultiplyBase100(
            const DigitsBuffer& leftDigits,
            const DigitsBuffer& rightDigits

        );
        static constexpr Comparators::Comparator CompareDigitsArray(
            const DigitsBuffer& lhs,
            const DigitsBuffer& rhs
        );
        static constexpr void SubtractDigits(
            DigitsBuffer& lhs,
            const DigitsBuffer& rhs
        );
        static constexpr DigitsBuffer DivideDigits(
            const DigitsBuffer& lhs,
            const DigitsBuffer& rhs
        );

        static constexpr DigitsBuffer LongDivide(
            const DigitsBuffer& dividend,
            const DigitsBuffer& divisor,
            DigitsBuffer& remainderOut
        );
        static constexpr DataBuffer Pack(
            const DigitsBuffer& digits,
            bool isPositive,
            fraction_index_t fractionIndex
        );

        static constexpr DataBuffer Pack(
            const MultiplicationDigitsBuffer& digits,
            bool isPositive,
            fraction_index_t fractionIndex
        );

        static constexpr fraction_index_t DetermineResultFractionIndex(
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );

        static constexpr Decimal Add(
            DataBuffer& lhs,
            DataBuffer& rhs,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static constexpr void PadFractionalParts(
            DataBuffer& left,
            DataBuffer& right,
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );

        static constexpr void PadNonFractionalParts(
            DataBuffer& left,
            DataBuffer& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static constexpr Decimal Subtract(
            DataBuffer& lhs,
            const DataBuffer& rhs,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static constexpr Decimal Multiply(
            const DataBuffer& lhs,
            const DataBuffer& rhs,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        template<DecimalRoundingMode Mode>
        static constexpr Decimal Divide(
            const DataBuffer& dividend,
            const DataBuffer& divisor,
            bool isPositive
        );

        template <IsDecimalBuffer T>
        static constexpr void TrimLeadingZeros(
            T& digits,
            fraction_index_t& fractionIndex
        );

        template <IsDecimalBuffer T>
        static constexpr void TrimTrailingZeros(
            T& digits,
            fraction_index_t fractionIndex
        );

        template <IsDecimalBuffer T>
        static constexpr void PadDecimalParts(
            T& digits,
            fraction_index_t& fractionIndex
        );

        [[nodiscard]] static constexpr byte_t CreateSignAndFractionByte(
            const bool isPositive,
            const fraction_index_t fractionIndex
        ){
            return (isPositive << 7) | (fractionIndex & 0x7F);
        }

        [[nodiscard]] static constexpr Comparators::Comparator Compare(const Decimal& lhs, const Decimal& rhs);
        template<IsDecimalBuffer T>
        [[nodiscard]] static constexpr bool IsZeroMagnitude(const  T& buffer);
        [[nodiscard]] static constexpr Comparators::Comparator CompareMagnitude(
            const DataBuffer& lhs,
            const DataBuffer& rhs,
            Int size
        );

        template<IsDecimalBuffer BUFFER, DecimalRoundingMode Mode>
        static constexpr bool DetermineRoundUp(
            const BUFFER& digits,
            Int digitsToKeep
        );

        template<IsDecimalBuffer BUFFER, DecimalRoundingMode Mode>
        static constexpr void RoundToScale(
            BUFFER& digits,
            fraction_index_t& fractionIndex,
            Int targetScale
        );

        static constexpr void RoundToScale(
            MultiplicationDigitsBuffer& digits,
            fraction_index_t& fractionIndex,
            Int targetScale,
            DecimalRoundingMode mode
        );

    public:
        constexpr Decimal();

        template <IsInteger T>
        explicit constexpr Decimal(T value);

        explicit constexpr Decimal(const byte_t* data, Int dataSize);
        explicit constexpr Decimal(const StringView& value);

        [[nodiscard]] constexpr bool IsPositive() const;
        [[nodiscard]] constexpr fraction_index_t GetFractionIndex() const;
        [[nodiscard]] String ToString(const ::Memory::IAllocator* allocator) const;
        [[nodiscard]] constexpr StringBuffer ToBufferString() const;

        [[nodiscard]] constexpr const byte_t* RawData() const;
        [[nodiscard]] constexpr Int RawSize() const;

        [[nodiscard]] constexpr const DataBuffer& Data() const;
        [[nodiscard]] static constexpr Int Size(Int precision);

        [[nodiscard]] double ToDouble() const;

        constexpr friend std::ostream& operator<<(std::ostream& os, const Decimal& decimal);

        constexpr friend Decimal operator+(const Decimal& left, const Decimal& right);
        constexpr friend Decimal operator-(const Decimal& left, const Decimal& right);
        constexpr friend Decimal operator*(const Decimal& left, const Decimal& right);
        constexpr friend Decimal operator/(const Decimal& left, const Decimal& right);

        constexpr friend bool operator==(const Decimal& left, const Decimal& right);
        constexpr friend bool operator>=(const Decimal& left, const Decimal& right);
        constexpr friend bool operator>(const Decimal& left, const Decimal& right);
        constexpr friend bool operator<(const Decimal& left, const Decimal& right);
        constexpr friend bool operator<=(const Decimal& left, const Decimal& right);

        template <IsInteger T>
        constexpr Decimal& operator=(T value);

        template <IsInteger T>
        [[nodiscard]] constexpr T To();

        template<DecimalRoundingMode T>
        static constexpr void RoundToScale(Decimal& value, Int precision);
    };
}

namespace DataTypes{
    template <IsDecimalBuffer T>
    constexpr void Decimal::TrimLeadingZeros(T& digits, fraction_index_t& fractionIndex){
        auto leadingZeros = 0;
        while (leadingZeros < fractionIndex && digits[leadingZeros] == DECIMAL_ZERO)
            leadingZeros++;

        if (leadingZeros == 0) return;

        digits.Remove(0, leadingZeros);
        fractionIndex -= static_cast<fraction_index_t>(leadingZeros);
    }

    template <IsDecimalBuffer T>
    constexpr void Decimal::TrimTrailingZeros(T& digits, fraction_index_t fractionIndex){
        for (auto i = digits.Size() - 1; i > fractionIndex; --i) {
            if (digits[i] != 0) break;
            digits.Pop();
        }
    }

    template <IsDecimalBuffer T>
    constexpr void Decimal::PadDecimalParts(T& digits, fraction_index_t& fractionIndex){
        if (fractionIndex % 2 != 0) {
            digits.Insert(0, 0);
            fractionIndex++;
        }

        const auto fractionalPart = digits.Size() - fractionIndex;
        if (fractionalPart % 2 != 0) digits.Push(DECIMAL_ZERO);
    }

    template <IsDecimalBuffer T>
    constexpr bool Decimal::IsZeroMagnitude(const T& buffer){
        Int i = 0;
        while (i < buffer.Size() && buffer[i] == 0) i++;
        return i == buffer.Size();
    }

    template <IsDecimalBuffer BUFFER, DecimalRoundingMode Mode>
    constexpr bool Decimal::DetermineRoundUp(
        const BUFFER& digits,
        const Int digitsToKeep
    ){
        if constexpr (Mode == DecimalRoundingMode::Truncate)
            return false;
        else{
            const auto firstDigitDropped = digits[digitsToKeep];
            if constexpr (Mode == DecimalRoundingMode::HalfUp)
                return firstDigitDropped >= 5;
            else if constexpr (Mode == DecimalRoundingMode::HalfEven){
                if (firstDigitDropped < 5)
                    return false;

                if (firstDigitDropped > 5)
                    return true;

                auto restNonZero = false;
                for (Int i = digitsToKeep + 1; i < digits.Size(); i++)
                    if (digits[i] != 0){
                        restNonZero = true;
                        break;
                    }
                return restNonZero || (digitsToKeep > 0 && (digits[digitsToKeep - 1] & 1));
            }
            else
                static_assert(DataTypes::AlwaysFalse<Decimal>, "Decimal::DetermineRoundUp: Invalid mode given");
        }
    }

    template <IsDecimalBuffer BUFFER, DecimalRoundingMode Mode>
    constexpr void Decimal::RoundToScale(
        BUFFER& digits,
        fraction_index_t& fractionIndex,
        const Int targetScale
    ){
        if (digits.Size() <= fractionIndex + targetScale)
            return;

        const auto digitsToKeep = fractionIndex + targetScale;
        const auto roundUp = Decimal::DetermineRoundUp<BUFFER, Mode>(digits, digitsToKeep);

        while (digits.Size() > digitsToKeep)
            digits.Pop();

        if (roundUp){
            Int i = digitsToKeep - 1;
            for (; i >= 0; i--){
                if (++digits[i] < 10)
                    break;
                digits[i] = 0;
            }

            if (i < 0){
                digits.Insert(0, 1);
                fractionIndex++;
            }
        }
    }

template <IsInteger T>
constexpr Decimal::Decimal(T value){
    static_assert(std::is_integral_v<T>, "Decimal::Decimal(T value): T must be an integer");

    const auto isPositive = value >= 0;
    auto absValue = Math::Abs<T>(value);

    Int numberOfDigits = 1;
    for (auto tempVal = absValue / 10; tempVal > 0; tempVal /= 10)
        numberOfDigits++;

    const auto leadingZeros = numberOfDigits % 2;
    const fraction_index_t fractionIndex = numberOfDigits + leadingZeros;

    DigitsBuffer digits;
    digits.SetSize(fractionIndex);

    for (Int i = fractionIndex - 1; i >= leadingZeros; --i){
        digits[i] = static_cast<Int>(absValue % 10);
        absValue /= 10;
    }

    this->_data = Decimal::Pack(digits, isPositive, fractionIndex);
}

template <IsInteger T>
constexpr Decimal& Decimal::operator=(T value){
    static_assert(std::is_integral_v<T>, "Decimal::operator=(T value): T must be an integer");
    *this = Decimal(value);
    return *this;
}

template <IsInteger T>
constexpr T Decimal::To(){
    const auto positive = this->IsPositive();

    using UnsignedType = std::make_unsigned_t<T>;
    UnsignedType mag = 0;

    for (Int i = DECIMAL_DIGITS_START_INDEX; i < this->_data.Size(); ++i)
        mag = mag * 10 + static_cast<UnsignedType>(this->_data[i] - '0');

    return positive ? static_cast<T>(mag) : -static_cast<T>(mag);
}

constexpr void Decimal::RoundToScale(
    MultiplicationDigitsBuffer& digits,
    fraction_index_t& fractionIndex,
    const Int targetScale,
    const DecimalRoundingMode mode
){
    switch (mode){
    case DecimalRoundingMode::Truncate:
        return Decimal::RoundToScale<MultiplicationDigitsBuffer, DecimalRoundingMode::Truncate>(digits, fractionIndex, targetScale);
    case DecimalRoundingMode::HalfUp:
        return Decimal::RoundToScale<MultiplicationDigitsBuffer, DecimalRoundingMode::HalfUp>(digits, fractionIndex, targetScale);
    case DecimalRoundingMode::HalfEven:
        return Decimal::RoundToScale<MultiplicationDigitsBuffer, DecimalRoundingMode::HalfEven>(digits, fractionIndex, targetScale);
    }
}

constexpr Decimal::Decimal() {
    this->_data.Push(Decimal::CreateSignAndFractionByte(true, 0));
    this->_data.Push(DECIMAL_ZERO);
}

constexpr Decimal::Decimal(const StringView& value){
    if (value.Empty()){
        *this = Decimal();
        return;
    }

    Int i = 0;
    const bool isPositive = value[0] != '-';
    if (value[0] == '-' || value[0] == '+')
        i++;

    while (i + 1 < value.Size() && value[i] == '0' && value[i + 1] != '.')
        i++;

    Int intCount = 0, fractionalCount = 0;
    bool hasDot = false;
    for (Int k = i; k < value.Size(); k++) {
        if (value[k] == '.')
            hasDot = true;
        else if (!hasDot)
            intCount++;
        else
            fractionalCount++;
    }

    const auto leadingPad = intCount % 2;
    const auto fractionalPad = fractionalCount % 2;

    const fraction_index_t fractionIndex = intCount + leadingPad;
    const Int totalSize = fractionIndex + fractionalPad + fractionalCount;

    DigitsBuffer digits;
    digits.SetSize(totalSize);

    Int digitsAfterPad = leadingPad;
    for (Int k = i; k < value.Size(); k++){
        if (value[k] == '.')
            continue;
        digits[digitsAfterPad++] = value[k] - '0';
    }

    this->_data = Decimal::Pack(digits, isPositive, fractionIndex);
}

constexpr Decimal::Decimal(const byte_t* data, const Int dataSize){
    this->_data.SetData(data, dataSize);
}

constexpr DataBuffer Decimal::Pack(const DigitsBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
    DataBuffer result;
    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

    for (Int i = 0; i < digits.Size(); i += 2) {
        const Int high = digits[i];
        const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
        result.Push((high << 4) | low);
    }

    return result;
}

constexpr DataBuffer Decimal::Pack(
    const MultiplicationDigitsBuffer& digits,
    const bool isPositive,
    const fraction_index_t fractionIndex
){
    DataBuffer result;
    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

    for (Int i = 0; i < digits.Size(); i += 2) {
        const Int high = digits[i];
        const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
        result.Push((high << 4) | low);
    }

    return result;
}

constexpr Decimal operator+(const Decimal &left, const Decimal &right){
    const auto leftSign = left.IsPositive();
    const auto rightSign = right.IsPositive();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    const auto fractionIndex = Math::Max<fraction_index_t>(leftFractionIndex, rightFractionIndex);

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    //both same sign, add them and use the sign afterward
    if (leftSign == rightSign)
        return Decimal::Add(leftCopy, rightCopy, fractionIndex, leftSign);

    //else they have different signs,
    //so subtract them
    const auto comparisonResult = Decimal::CompareMagnitude(leftCopy, rightCopy, leftCopy.Size() - 1);

    if (comparisonResult == Comparators::Comparator::Equal)
        return Decimal();

    return comparisonResult == Comparators::Comparator::Greater
               ? Decimal::Subtract(leftCopy, rightCopy, fractionIndex, leftSign)
               : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, rightSign);
}

constexpr Decimal operator-(const Decimal &left, const Decimal &right){
    const bool leftSign = left.IsPositive();
    const bool rightSign = right.IsPositive();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    const auto fractionIndex = Math::Max<fraction_index_t>(leftFractionIndex, rightFractionIndex);

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    if (leftSign == rightSign){
        const auto comparisonResult = Decimal::CompareMagnitude(leftCopy, rightCopy, leftCopy.Size() - 1);

        if (comparisonResult == Comparators::Comparator::Equal)
            return Decimal();

        return comparisonResult == Comparators::Comparator::Greater
                   ? Decimal::Subtract(leftCopy, rightCopy, fractionIndex, leftSign)
                   : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, !leftSign);
    }

    return Decimal::Add(leftCopy, rightCopy, fractionIndex, leftSign);
}

constexpr Decimal operator*(const Decimal &left, const Decimal &right){
    return Decimal::Multiply(
        left.Data(),
        right.Data(),
        left.GetFractionIndex() + right.GetFractionIndex(),
        left.IsPositive() == right.IsPositive()
    );
}

constexpr Decimal operator/(const Decimal& left, const Decimal& right){
    return Decimal::Divide<DecimalRoundingMode::HalfUp>(
        left.Data(),
        right.Data(),
        left.IsPositive() == right.IsPositive()
    );
}

constexpr Int Decimal::Size(const Int precision){
    return precision / 2 + 1 + 1; //+1 for sign and fraction index, +1 for alignment
}

constexpr bool operator==(const Decimal& left, const Decimal& right){
    return Decimal::Compare(left, right) == Comparators::Comparator::Equal;
}

constexpr bool operator>=(const Decimal& left, const Decimal& right){
    return Decimal::Compare(left, right) >= Comparators::Comparator::Equal;
}

constexpr bool operator>(const Decimal& left, const Decimal& right){
    return Decimal::Compare(left, right) == Comparators::Comparator::Greater;
}

constexpr bool operator<(const Decimal& left, const Decimal& right){
    return Decimal::Compare(left, right) == Comparators::Comparator::Less;
}

constexpr bool operator<=(const Decimal& left, const Decimal& right){
    return Decimal::Compare(left, right) <= Comparators::Comparator::Equal;
}

constexpr bool Decimal::IsPositive() const { return ( this->_data[DECIMAL_HEADER_INDEX] >> 7 ) & 0x01; }

constexpr fraction_index_t Decimal::GetFractionIndex() const { return static_cast<fraction_index_t>(this->_data[DECIMAL_HEADER_INDEX] & 0x7F); }

constexpr const byte_t* Decimal::RawData() const { return this->_data.Data(); }

constexpr Int Decimal::RawSize() const { return this->_data.Size(); }

constexpr const DataBuffer& Decimal::Data() const { return this->_data; }

constexpr StringBuffer Decimal::ToBufferString() const{
    const auto isPositive = this->IsPositive();

    StringBuffer buffer;

    if (!isPositive) buffer.Push('-');

    const fraction_index_t fractionIndex = Decimal::GetFractionIndex() + !isPositive;

    Int strCounter = !isPositive;
    for (int i = DECIMAL_DIGITS_START_INDEX; i < this->_data.Size(); i++){
        if (strCounter == fractionIndex && fractionIndex != 0)
            buffer.Push('.');

        buffer.Push(static_cast<char>('0' + ((this->_data[i] >> 4) & 0x0F)));
        strCounter++;

        if (strCounter == fractionIndex && fractionIndex != 0)
            buffer.Push('.');

        buffer.Push(static_cast<char>('0' + (this->_data[i] & 0x0F)));
        strCounter++;
    }

    if (buffer.Last() == '0') buffer.Pop();

    return buffer;
}

constexpr Comparators::Comparator Decimal::Compare(const Decimal& lhs, const Decimal& rhs){
    const auto leftSign = lhs.IsPositive();
    const auto rightSign = rhs.IsPositive();

    if (leftSign != rightSign)
        return Comparators::Compare<bool>(leftSign, rightSign);

    const auto leftFractionIndex = lhs.GetFractionIndex();
    const auto rightFractionIndex = rhs.GetFractionIndex();

    if (leftFractionIndex != rightFractionIndex)
        return Comparators::Compare<fraction_index_t>(leftFractionIndex, rightFractionIndex);

    const auto& leftData = lhs.Data();
    const auto& rightData = rhs.Data();

    const auto commonSize = Math::Min<Int>(leftData.Size(), rightData.Size()) - 1;
    const auto result = Decimal::CompareMagnitude(leftData, rightData, commonSize);
    if (result != Comparators::Comparator::Equal)
        return result;

    return Comparators::Compare<Int>(leftData.Size(), rightData.Size());
}

constexpr Comparators::Comparator Decimal::CompareMagnitude(
    const DataBuffer& lhs,
    const DataBuffer& rhs,
    const Int size
){
    if consteval {
        for (Int i = 1; i <= size; ++i)
            if (lhs[i] != rhs[i])
                return Comparators::Compare<UnsignedTinyInt>(lhs[i], rhs[i]);
        return Comparators::Comparator::Equal;
    }

    const auto result = std::memcmp(
        lhs.Data() + DECIMAL_DIGITS_START_INDEX,
        rhs.Data() + DECIMAL_DIGITS_START_INDEX,
        static_cast<size_t>(size)
    );
    return Comparators::Compare<Int>(result, 0);
}

constexpr Int Decimal::LowNibble(const DataBuffer& buffer, const Int index){
    return (buffer[index] & 0x0F);
}

constexpr Int Decimal::HighNibble(const DataBuffer& buffer, const Int index){
    return (buffer[index] >> 4);
}

constexpr byte_t Decimal::PackNibbles(const Int highNibble, const Int lowNibble){
    return static_cast<byte_t>((highNibble << 4) | lowNibble);
}

constexpr DigitsBuffer Decimal::Unpack(const DataBuffer& bytes){
    DigitsBuffer digits;

    for (Int i = DECIMAL_DIGITS_START_INDEX; i < bytes.Size(); i++) {
        digits.Push(Decimal::HighNibble(bytes, i));
        digits.Push(Decimal::LowNibble(bytes, i));
    }

    return digits;
}

constexpr DigitsBuffer Decimal::ToBase100(const DataBuffer& bytes){
    DigitsBuffer digits;
    for (Int i = DECIMAL_DIGITS_START_INDEX; i < bytes.Size(); i++)
        digits.Push(Decimal::HighNibble(bytes, i) * 10 + Decimal::LowNibble(bytes, i));

    return digits;
}

constexpr MultiplicationDigitsBuffer Decimal::Base100ToDigits(const MultiplicationDigitsBuffer& input){
    MultiplicationDigitsBuffer digits;
    for (Int i = 0; i < input.Size(); ++i) {
        digits.Push(input[i] / 10);
        digits.Push(input[i] % 10);
    }
    return digits;
}

constexpr MultiplicationDigitsBuffer Decimal::MultiplyBase100(
    const DigitsBuffer& leftDigits,
    const DigitsBuffer& rightDigits
){
    MultiplicationDigitsBuffer result;
    result.SetSize(leftDigits.Size() + rightDigits.Size());

    for (Int i = leftDigits.Size() - 1; i >= 0; --i) {
        const Int ld = leftDigits[i];
        if (ld == 0) continue;                      // skip zero BCD digit – no contribution

        for (Int j = rightDigits.Size() - 1; j >= 0; --j) {
            const Int rd = rightDigits[j];
            if (rd == 0) continue;                  // skip zero BCD digit – no contribution

            result[i + j + 1] += ld * rd;
        }
    }

    for (Int k = result.Size() - 1; k > 0; k--) {
        if (result[k] >= 100) {
            result[k - 1] += result[k] / 100;
            result[k] %= 100;
        }
    }

    return result;
}

constexpr Comparators::Comparator Decimal::CompareDigitsArray(
    const DigitsBuffer& lhs,
    const DigitsBuffer& rhs
){
    Int lhsI = 0;
    while (lhsI < lhs.Size() && lhs[lhsI] == 0) lhsI++;

    Int rhsI = 0;
    while (rhsI < rhs.Size() && rhs[rhsI] == 0) rhsI++;

    const auto lhsSize = lhs.Size() - lhsI;
    const auto rhsSize = rhs.Size() - rhsI;

    if (lhsSize != rhsSize)
        return Comparators::Compare(lhsSize, rhsSize);

    for (; lhsI < lhs.Size(); lhsI++)
        if (lhs[lhsI] != rhs[rhsI++])
            return Comparators::Compare(lhs[lhsI],  rhs[rhsI++]);

    return Comparators::Comparator::Equal;
}

constexpr void Decimal::SubtractDigits(
    DigitsBuffer& lhs,
    const DigitsBuffer& rhs
){
    Int lhsI = lhs.Size() - 1;
    Int rhsI = rhs.Size() - 1;
    Int borrow = 0;

    for (; lhsI >= 0 && rhsI >= 0; --lhsI, --rhsI){
        Int d = lhs[lhsI] - rhs[rhsI] - borrow;
        if (d < 0){
            d += 10;
            borrow = 1;
        }
        else borrow = 0;

        lhs[lhsI] = d;
    }

    for (; lhsI >= 0 && borrow; --lhsI){
        Int d = lhs[lhsI] - borrow;
        if (d < 0){
            d += 10;
            borrow = 1;
        }
        else borrow = 0;
        lhs[lhsI] = d;
    }
}

constexpr DigitsBuffer Decimal::DivideDigits(
    const DigitsBuffer& lhs,
    const DigitsBuffer& rhs
){
    DigitsBuffer result;
    result.SetSize(lhs.Size() + rhs.Size());

    for (Int i = lhs.Size() - 1; i >= 0; --i) {
        for (Int j = rhs.Size() - 1; j >= 0; --j) {
            const auto pos = i + j + 1;
            result[pos] += lhs[i] * rhs[j];
        }
    }

    // Step 3: fix carries
    for (Int k = result.Size() - 1; k > 0; k--) {
        result[k-1] += result[k] / 10;
        result[k] %= 10;
    }

    return result;
}

constexpr DigitsBuffer Decimal::LongDivide(
    const DigitsBuffer& dividend,
    const DigitsBuffer& divisor,
    DigitsBuffer& remainderOut
){
    DigitsBuffer quotient;
    quotient.SetSize(dividend.Size());

    for (Int i = 0; i < dividend.Size(); ++i){
        remainderOut.Push(dividend[i]);

        Int quotientDigit = 0;
        while (Decimal::CompareDigitsArray(remainderOut, divisor) != Comparators::Comparator::Less){
            Decimal::SubtractDigits(remainderOut, divisor);
            ++quotientDigit;
        }
        quotient[i] = quotientDigit;
    }

    return quotient;
}

constexpr fraction_index_t Decimal::DetermineResultFractionIndex(
    const fraction_index_t leftFractionIndex,
    const fraction_index_t rightFractionIndex
){
    return Math::Max<fraction_index_t>(leftFractionIndex, rightFractionIndex);
}

constexpr Decimal Decimal::Add(
    DataBuffer& lhs,
    DataBuffer& rhs,
    fraction_index_t fractionIndex,
    const bool isPositive
){
    lhs.Insert(DECIMAL_DIGITS_START_INDEX, DECIMAL_ZERO);
    rhs.Insert(DECIMAL_DIGITS_START_INDEX, DECIMAL_ZERO);
    fractionIndex += 2;

    Int carry = 0;
    for (auto i = lhs.Size() - 1; i > 0; i--){
        const Int lowNibble = Decimal::LowNibble(lhs, i) + Decimal::LowNibble(rhs, i) + carry;
        const Int highNibble = Decimal::HighNibble(lhs, i) + Decimal::HighNibble(rhs, i) + (lowNibble / 10);
        carry = highNibble / 10;
        lhs[i] = Decimal::PackNibbles(highNibble % 10, lowNibble % 10);
    }

    Decimal::TrimLeadingZeros<DataBuffer>(lhs, fractionIndex);
    lhs[DECIMAL_HEADER_INDEX] = Decimal::CreateSignAndFractionByte(isPositive, fractionIndex);
    return Decimal(lhs.Data(), lhs.Size());
}

constexpr void Decimal::PadFractionalParts(
    DataBuffer& left, DataBuffer& right,
    const fraction_index_t leftFractionIndex,
    const fraction_index_t rightFractionIndex
){
    const auto leftFracDigits = left.Size() * 2 - leftFractionIndex;
    const auto rightFracDigits = right.Size() * 2 - rightFractionIndex;
    const auto maxFracDigits = std::max(leftFracDigits, rightFracDigits);

    if (leftFracDigits < maxFracDigits) {
        const auto digitsToAdd = maxFracDigits - leftFracDigits;
        const auto bytesToAdd = (digitsToAdd + 1) / 2;
        for (auto i = 0; i < bytesToAdd; i++)
            left.Push(DECIMAL_ZERO);
    }

    if (rightFracDigits < maxFracDigits) {
        const auto digitsToAdd = maxFracDigits - rightFracDigits;
        const auto bytesToAdd = (digitsToAdd + 1) / 2;
        for (auto i = 0; i < bytesToAdd; i++)
            right.Push(DECIMAL_ZERO);
    }
}

constexpr void Decimal::PadNonFractionalParts(
    DataBuffer& left,
    DataBuffer& right,
    fraction_index_t& leftFractionIndex,
    fraction_index_t& rightFractionIndex
){
    const auto leftNonFracSize = leftFractionIndex / 2;
    const auto rightNonFracSize = rightFractionIndex / 2;
    const auto maxNonFracSize = std::max(leftNonFracSize, rightNonFracSize);

    // Pad left with leading zeros if needed
    if (leftNonFracSize < maxNonFracSize) {
        left.Insert(DECIMAL_DIGITS_START_INDEX, maxNonFracSize - leftNonFracSize, DECIMAL_ZERO);
        leftFractionIndex += static_cast<fraction_index_t>((maxNonFracSize - leftNonFracSize) * 2);
    }

    // Pad right with leading zeros if needed
    if (rightNonFracSize < maxNonFracSize) {
        right.Insert(DECIMAL_DIGITS_START_INDEX, maxNonFracSize - rightNonFracSize, DECIMAL_ZERO);
        rightFractionIndex += static_cast<fraction_index_t>((maxNonFracSize - rightNonFracSize) * 2);
    }
}

constexpr Decimal Decimal::Subtract(
    DataBuffer& lhs,
    const DataBuffer& rhs,
    fraction_index_t fractionIndex,
    const bool isPositive
){
    Int borrow = 0;
    for (Int i = lhs.Size() - 1; i > 0; i--) {
        Int lowNibble = Decimal::LowNibble(lhs, i) - Decimal::LowNibble(rhs, i) - borrow;
        if (lowNibble < 0){
            lowNibble += 10;
            borrow = 1;
        }
        else borrow = 0;

        Int highNibble = Decimal::HighNibble(lhs, i) - Decimal::HighNibble(rhs, i) - borrow;
        if (highNibble < 0){
            highNibble += 10;
            borrow = 1;
        }
        else borrow = 0;

        lhs[i] = Decimal::PackNibbles(highNibble, lowNibble);
    }
    Decimal::TrimLeadingZeros(lhs, fractionIndex);
    lhs[DECIMAL_HEADER_INDEX] = Decimal::CreateSignAndFractionByte(isPositive, fractionIndex);

    return Decimal(lhs.Data(), lhs.Size());
}

constexpr Decimal Decimal::Multiply(
    const DataBuffer& lhs,
    const DataBuffer& rhs,
    fraction_index_t fractionIndex,
    const bool isPositive
){

    const auto leftUnpacked = Decimal::ToBase100(lhs);
    const auto rightUnpacked = Decimal::ToBase100(rhs);
    const auto productUnpacked = Decimal::MultiplyBase100(leftUnpacked, rightUnpacked);

    auto productDigits = Decimal::Base100ToDigits(productUnpacked);
    Decimal::TrimLeadingZeros(productDigits, fractionIndex);
    Decimal::TrimTrailingZeros(productDigits, fractionIndex);

    const Int maxScale = DECIMAL_MAX_NUMBER_OF_DIGITS - 3 - fractionIndex;
    if (maxScale < 0)
        throw std::runtime_error("Decimal multiplication overflow");

    Decimal::RoundToScale<MultiplicationDigitsBuffer, DecimalRoundingMode::HalfUp>(productDigits, fractionIndex, maxScale);
    Decimal::PadDecimalParts<MultiplicationDigitsBuffer>(productDigits, fractionIndex);
    const auto packed = Decimal::Pack(productDigits, isPositive, fractionIndex);
    return Decimal(packed.Data(), packed.Size());
}

template <DecimalRoundingMode Mode>
constexpr Decimal Decimal::Divide(
    const DataBuffer& dividend,
    const DataBuffer& divisor,
    const bool isPositive
){

    auto leftDigits  = Decimal::Unpack(dividend);
    auto rightDigits = Decimal::Unpack(divisor);

    if (Decimal::IsZeroMagnitude<DigitsBuffer>(rightDigits))
        throw std::runtime_error("Decimal division by zero");

    const Int leftFractionIndex = dividend[DECIMAL_HEADER_INDEX] & 0x7F;
    const Int rightFractionIndex = divisor [DECIMAL_HEADER_INDEX] & 0x7F;

    const Int leftFractionalSize = leftDigits.Size()  - leftFractionIndex;
    const Int rightFractionalSize = rightDigits.Size() - rightFractionIndex;

    const Int targetScale = Math::Max<Int>(leftFractionalSize, rightFractionalSize);
    const Int shift = rightFractionalSize - leftFractionalSize + targetScale + 1;
    for (Int k = 0; k < shift; ++k)
        leftDigits.Push(0);

    DigitsBuffer remainderOut;
    auto quotient = Decimal::LongDivide(
        leftDigits,
        rightDigits,
        remainderOut
    );

    fraction_index_t fractionIndex = quotient.Size() - (targetScale + 1);
    Decimal::TrimLeadingZeros<DigitsBuffer>(quotient, fractionIndex);

    if (!Decimal::IsZeroMagnitude<DigitsBuffer>(remainderOut))
        quotient.Push(1);

    Decimal::RoundToScale<DigitsBuffer, Mode>(quotient, fractionIndex, targetScale);

    Decimal::TrimTrailingZeros<DigitsBuffer>(quotient, fractionIndex);
    Decimal::PadDecimalParts<DigitsBuffer>(quotient, fractionIndex);

    const auto sign = isPositive || Decimal::IsZeroMagnitude<DigitsBuffer>(quotient);
    const auto packed = Decimal::Pack(quotient, sign, fractionIndex);
    return Decimal(packed.Data(), packed.Size());
}

constexpr std::ostream & operator<<(std::ostream &os, const Decimal &decimal){
    const auto buffer = decimal.ToBufferString();
    os.write(buffer.Data(), buffer.Size());
    return os;
}
}

template<> struct std::numeric_limits<DataTypes::Decimal> {
    static constexpr bool is_specialized = true;

    static constexpr DataTypes::Decimal min() noexcept {
        static constexpr DataTypes::StringView MIN_VIEW = "-9999999999999999.9999";
        static constexpr DataTypes::Decimal VALUE(MIN_VIEW);
        return VALUE;
    }

    static constexpr DataTypes::Decimal max() noexcept {
        static constexpr DataTypes::StringView MAX_VIEW = "9999999999999999.9999";
        static constexpr DataTypes::Decimal VALUE(MAX_VIEW);
        return VALUE;
    }

    static constexpr int digits10 = 34;  // max base-10 precision
    static constexpr int radix = 10;     // base of your decimal
};

