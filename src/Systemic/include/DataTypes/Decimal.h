#pragma once

#include "../Comparators.h"
#include "String.h"
#include "../DataStructures/StaticArray.h"
#include "../Functions/MathFunctions.h"
#include "DataTypes.h"

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
        using StringBuffer = DataStructures::StaticArray<char, 2 * DECIMAL_ARRAY_SIZE>;
        using DigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_TEMPORARY_BUFFER_SIZE>;
        using MultiplicationDigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_MULTIPLICATION_BUFFER_SIZE>;

    protected:
        static constexpr DigitsBuffer Unpack(const DataBuffer& bytes);
        static constexpr  MultiplicationDigitsBuffer MultiplyDigits(
            const DigitsBuffer& leftDigits,
            const DigitsBuffer& rightDigits
        );
        static constexpr MultiplicationDigitsBuffer DivideDigits(
            const DigitsBuffer& leftDigits,
            const DigitsBuffer& rightDigits
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

        static constexpr Decimal Divide(
            const DataBuffer& left,
            const DataBuffer& right,
            fraction_index_t& fractionIndex,
            bool isPositive
        );

        static constexpr void TrimLeadingZeros(
            MultiplicationDigitsBuffer& digits,
            fraction_index_t& fractionIndex
        );

        static constexpr void TrimLeadingZeros(
            DataBuffer& digits,
            fraction_index_t& fractionIndex
        );

        static constexpr void TrimTrailingZeros(
            MultiplicationDigitsBuffer& digits,
            fraction_index_t fractionIndex
        );

        static constexpr void PadDecimalParts(
            MultiplicationDigitsBuffer& digits,
            fraction_index_t& fractionIndex
        );

        [[nodiscard]] static constexpr byte_t CreateSignAndFractionByte(
            const bool isPositive,
            const fraction_index_t fractionIndex
        ){
            return (isPositive << 7) | (fractionIndex & 0x7F);
        }

        [[nodiscard]] static constexpr Comparators::Comparator Compare(const Decimal& lhs, const Decimal& rhs);

        [[nodiscard]] static constexpr Comparators::Comparator CompareMagnitude(
            const DataBuffer& lhs,
            const DataBuffer& rhs,
            Int size
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

        [[nodiscard]] constexpr const byte_t* GetRawData() const;
        [[nodiscard]] constexpr Int GetRawDataSize() const;

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

    };
// Specialization
}

namespace DataTypes{
template <IsInteger T>
constexpr Decimal::Decimal(T value){
    static_assert(std::is_integral_v<T>, "Decimal::Decimal(T value): T must be an integer");

    const auto isPositive = value >= 0;
    auto absValue = Math::Abs<T>(value);


    Int numberOfDigits = 0;
    for (auto tempVal = absValue / 10; tempVal > 0; tempVal /= 10)
        numberOfDigits++;

    const auto leadingZeros = numberOfDigits % 2;
    const fraction_index_t fractionIndex = numberOfDigits + leadingZeros;

    DigitsBuffer digits;
    digits.SetSize(fractionIndex + numberOfDigits + 2);

    for (Int i = leadingZeros + numberOfDigits - 1; i >= leadingZeros; --i){
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

constexpr Decimal::Decimal() {
    this->_data.Push(Decimal::CreateSignAndFractionByte(true, 0));
    this->_data.Push(DECIMAL_ZERO);
}

constexpr Decimal::Decimal(const StringView& value){
    if (value.Empty()) return;

    Int start = 0;

    const bool isPositive = value[0] != '-';
    if (value[0] == '-' || value[0] == '+')
        start++;

    while (start < value.Size() - 1 && value[start] == '0' && value[start + 1] != '.')
        start++;

    Int dotPos = -1;
    for (Int i = start; i < value.Size(); i++) {
        if (value[i] == '.'){
            dotPos = i;
            break;
        }
    }

    // Stack buffer — no heap
    DataStructures::StaticArray<char, DECIMAL_ARRAY_SIZE * 2> digitsArray;
    for (Int i = start; i < value.Size(); i++) {
        if (i == dotPos) continue;
        digitsArray.Push(value[i]);
    }

    fraction_index_t fractionIndex = (dotPos == -1)
        ? 0
        : static_cast<fraction_index_t>(dotPos - start);

    if ((digitsArray.Size() - fractionIndex) % 2 != 0)
        digitsArray.Push('0');

    if (fractionIndex % 2 != 0) {
        // shift right by 1
        for (Int i = digitsArray.Size(); i > 0; i--)
            digitsArray[i - 1] = digitsArray[i - 2];

        digitsArray.Insert('0', 0);
        fractionIndex++;
    }

    this->_data.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));
    const auto digitsCount = digitsArray.Size();

    for (Int i = 0; i < digitsCount; i += 2) {
        auto val = static_cast<byte_t>((digitsArray[i] - '0') << 4);

        if (i + 1 < digitsCount)
            val |= static_cast<byte_t>(digitsArray[i + 1] - '0');

        this->_data.Push(val);
    }
}

constexpr Decimal::Decimal(const byte_t* data, const Int dataSize){
    this->_data.SetData(data, dataSize);
}

constexpr Decimal::DataBuffer Decimal::Pack(const DigitsBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
    DataBuffer result;
    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

    for (Int i = 0; i < digits.Size(); i += 2) {
        const Int high = digits[i];
        const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
        result.Push((high << 4) | low);
    }

    return result;
}

constexpr Decimal::DataBuffer Decimal::Pack(
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
                   : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, rightSign);
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
    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    fraction_index_t fractionIndex = leftFractionIndex + rightFractionIndex;

    return Decimal::Divide(
        leftCopy,
        rightCopy,
        fractionIndex,
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

constexpr const byte_t* Decimal::GetRawData() const { return this->_data.Data(); }

constexpr Int Decimal::GetRawDataSize() const { return this->_data.Size(); }

constexpr const Decimal::DataBuffer& Decimal::Data() const { return this->_data; }

constexpr Decimal::StringBuffer Decimal::ToBufferString() const{
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

constexpr void Decimal::TrimLeadingZeros(MultiplicationDigitsBuffer& digits, fraction_index_t &fractionIndex){
    auto leadingZeros = 0;
    while (leadingZeros < fractionIndex && digits[leadingZeros] == 0)
        leadingZeros++;

    if (leadingZeros == 0) return;

    digits.Remove(0, leadingZeros);
    fractionIndex -= static_cast<fraction_index_t>(leadingZeros);
}

constexpr void Decimal::TrimLeadingZeros(DataBuffer& digits, fraction_index_t &fractionIndex){
    while (fractionIndex > 2 && digits[DECIMAL_DIGITS_START_INDEX] == DECIMAL_ZERO) {
        digits.Remove(DECIMAL_DIGITS_START_INDEX);
        fractionIndex -= 2;
    }
}

constexpr void Decimal::TrimTrailingZeros(MultiplicationDigitsBuffer& digits, const fraction_index_t fractionIndex){
    for (auto i = digits.Size() - 1; i > fractionIndex; i--) {
        if (digits[i] != 0) break;
        digits.Pop();
    }
}

constexpr void Decimal::PadDecimalParts(MultiplicationDigitsBuffer& digits, fraction_index_t& fractionIndex){
    if (fractionIndex % 2 != 0) {
        digits.Insert(0, 0);
        fractionIndex++;
    }

    const auto fractionalPart = digits.Size() - fractionIndex;
    if (fractionalPart % 2 != 0) digits.Push(DECIMAL_ZERO);
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

constexpr Decimal::DigitsBuffer Decimal::Unpack(const DataBuffer& bytes){
    DigitsBuffer digits;

    for (int i = 1; i < bytes.Size(); i++) {
        digits.Push((bytes[i] >> 4) & 0x0F);
        digits.Push(bytes[i] & 0x0F);
    }

    return digits;
}

constexpr Decimal::MultiplicationDigitsBuffer Decimal::MultiplyDigits(
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
        const Int val = result[k];
        if (val >= 10) {
            result[k - 1] += val / 10;
            result[k]      = val % 10;
        }
    }

    return result;
}

constexpr Decimal::MultiplicationDigitsBuffer Decimal::DivideDigits(
    const DigitsBuffer& leftDigits,
    const DigitsBuffer& rightDigits
){
    MultiplicationDigitsBuffer result;
    result.SetSize(leftDigits.Size() + rightDigits.Size());

    for (Int i = leftDigits.Size() - 1; i >= 0; --i) {
        for (Int j = rightDigits.Size() - 1; j >= 0; --j) {
            const auto pos = i + j + 1;
            result[pos] += leftDigits[i] * rightDigits[j];
        }
    }

    // Step 3: fix carries
    for (Int k = result.Size() - 1; k > 0; k--) {
        result[k-1] += result[k] / 10;
        result[k] %= 10;
    }

    return result;
}

constexpr fraction_index_t Decimal::DetermineResultFractionIndex(
    const fraction_index_t leftFractionIndex,
    const fraction_index_t rightFractionIndex){
    return std::max(leftFractionIndex, rightFractionIndex);
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
        const Int lowNibble = (lhs[i] & 0x0F) + (rhs[i] & 0x0F) + carry;
        const Int highNibble = (lhs[i] >> 4) + (rhs[i] >> 4) + (lowNibble / 10);
        carry = highNibble / 10;
        lhs[i] = static_cast<byte_t>((highNibble % 10) << 4 | lowNibble % 10);
    }

    Decimal::TrimLeadingZeros(lhs, fractionIndex);
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
        Int lowNibble = (lhs[i] & 0x0F) - (rhs[i] & 0x0F) - borrow;
        if (lowNibble < 0){
            lowNibble += 10;
            borrow = 1;
        }
        else borrow = 0;

        Int highNibble = (lhs[i] >> 4) - (rhs[i] >> 4) - borrow;
        if (highNibble < 0){
            highNibble += 10;
            borrow = 1;
        }
        else borrow = 0;

        lhs[i] = static_cast<byte_t>((highNibble << 4) | lowNibble);
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
    auto productDigits = Decimal::MultiplyDigits(
        Decimal::Unpack(lhs),
        Decimal::Unpack(rhs)
    );

    Decimal::TrimLeadingZeros(productDigits, fractionIndex);
    Decimal::TrimTrailingZeros(productDigits, fractionIndex);



    Decimal::PadDecimalParts(productDigits, fractionIndex);

    const auto packed = Decimal::Pack(productDigits, isPositive, fractionIndex);
    return Decimal(packed.Data(), packed.Size());
}

constexpr Decimal Decimal::Divide(
    const DataBuffer& left,
    const DataBuffer& right,
    fraction_index_t &fractionIndex,
    const bool isPositive
){
    const auto leftDigits  = Decimal::Unpack(left);
    const auto rightDigits = Decimal::Unpack(right);

    auto productDigits = Decimal::DivideDigits(leftDigits, rightDigits);

    Decimal::TrimLeadingZeros(productDigits, fractionIndex);
    Decimal::TrimTrailingZeros(productDigits, fractionIndex);
    Decimal::PadDecimalParts(productDigits, fractionIndex);

    const auto packed = Decimal::Pack(productDigits, isPositive, fractionIndex);
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

