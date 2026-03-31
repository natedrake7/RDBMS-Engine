#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "String.h"
#include "../DataStructures/StaticArray.h"
#include "../Functions/MathFunctions.h"

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
        using AdditionDigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_TEMPORARY_BUFFER_SIZE>;
        using MultiplicationDigitsBuffer = DataStructures::StaticArray<Int, DECIMAL_MULTIPLICATION_BUFFER_SIZE>;

        enum class ComparisonResult : TinyInt{
            Less = -1,
            Equal = 0,
            Greater = 1,
        };

    protected:
        template <typename T>
        constexpr void InitializeFromInteger(T value);

        static constexpr AdditionDigitsBuffer Unpack(const DataBuffer& bytes);
        static constexpr  MultiplicationDigitsBuffer MultiplyDigits(
            const AdditionDigitsBuffer& leftDigits,
            const AdditionDigitsBuffer& rightDigits
        );
        static constexpr DataBuffer Pack(
            const AdditionDigitsBuffer& digits,
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
        static constexpr ComparisonResult CompareDecimalsWithoutSign(
            const DataBuffer& leftData,
            const DataBuffer& rightData
        );

        static constexpr Decimal Add(
            const DataBuffer& left,
            const DataBuffer& right,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static constexpr void PadFractionalParts(
            DataBuffer& left, DataBuffer& right,
            fraction_index_t leftFractionIndex,
            fraction_index_t rightFractionIndex
        );

        static constexpr void PadNonFractionalParts(
            DataBuffer& left, DataBuffer& right,
            fraction_index_t& leftFractionIndex,
            fraction_index_t& rightFractionIndex
        );

        static constexpr Decimal Subtract(
            const DataBuffer& left, const DataBuffer& right,
            fraction_index_t fractionIndex,
            bool isPositive
        );

        static constexpr Decimal Multiply(
            const DataBuffer& left, const DataBuffer& right,
            fraction_index_t& fractionIndex,
            bool isPositive
        );

        static constexpr Decimal Divide(
            const std::vector<byte_t>& left,
            const std::vector<byte_t>& right,
            fraction_index_t& fractionIndex,
            bool isPositive
        );

        static constexpr void TrimLeadingZeros(
            MultiplicationDigitsBuffer& digits,
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

        [[nodiscard]] static constexpr byte_t CreateSignAndFractionByte(bool isPositive, fraction_index_t fractionIndex) ;

        [[nodiscard]] static constexpr bool IsGreaterMagnitude(const DataBuffer& left, const DataBuffer& right);

    public:
        constexpr Decimal();
        explicit constexpr Decimal(const byte_t* data, Int dataSize);
        explicit constexpr Decimal(bool value);
        explicit constexpr Decimal(TinyInt value);
        explicit constexpr Decimal(SmallInt value);
        explicit constexpr Decimal(Int value);
        explicit constexpr Decimal(BigInt value);
        explicit constexpr Decimal(const StringView& value);
        constexpr ~Decimal() = default;

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

        constexpr friend Decimal operator+(const Decimal& left, BigInt right);
        constexpr friend Decimal operator-(const Decimal& left, BigInt right);
        constexpr friend Decimal operator*(const Decimal& left, BigInt right);
        constexpr friend Decimal operator/(const Decimal& left, BigInt right);

        constexpr friend bool operator==(const Decimal& left, const Decimal& right);
        constexpr friend bool operator>=(const Decimal& left, const Decimal& right);
        constexpr friend bool operator>(const Decimal& left, const Decimal& right);
        constexpr friend bool operator<(const Decimal& left, const Decimal& right);
        constexpr friend bool operator<=(const Decimal& left, const Decimal& right);

        constexpr Decimal& operator=(TinyInt right);
        constexpr Decimal& operator=(SmallInt right);
        constexpr Decimal& operator=(Int right);
        constexpr Decimal& operator=(BigInt right);

    };
// Specialization
}

namespace DataTypes{
template <typename T>
constexpr void Decimal::InitializeFromInteger(const T value){
    static_assert(std::is_integral_v<T>, "Decimal::InitializeFromInteger: T must be an integer");

    const auto isPositive = value >= 0;
    auto absValue = Functions::Math::Abs(static_cast<BigInt>(value));

    AdditionDigitsBuffer digits;
    if (absValue == 0) digits.Push(DECIMAL_ZERO);

    while (absValue > 0) {
        digits.Push(absValue % 10);
        absValue /= 10;
    }

    std::ranges::reverse(digits);

    digits.Push(DECIMAL_ZERO);
    digits.Push(DECIMAL_ZERO);

    fraction_index_t fractionIndex = digits.Size() - 2;

    if (fractionIndex % 2 != 0) {
        digits.Insert(0, DECIMAL_ZERO);
        fractionIndex++;
    }

    this->_data = Decimal::Pack(digits, isPositive, fractionIndex);
}

constexpr Decimal::Decimal(const bool value){
    //boolean is always positive
    constexpr auto isPositive = true;

    //fraction index is always at a fixed position
    static constexpr fraction_index_t BOOLEAN_FRACTION_INDEX = 2;

    const auto signAndFractionPoint = Decimal::CreateSignAndFractionByte(isPositive, BOOLEAN_FRACTION_INDEX);

    this->_data.Push(signAndFractionPoint);
    byte_t val = 0;
    val |= (value ? 1 : 0);

    this->_data.Push(val);
    this->_data.Push(DECIMAL_ZERO);
}

constexpr Decimal::Decimal() {
    this->_data.Push(Decimal::CreateSignAndFractionByte(true, 0));
    this->_data.Push(DECIMAL_ZERO);
}

constexpr Decimal::Decimal(const TinyInt value){
    this->InitializeFromInteger<TinyInt>(value);
}

constexpr Decimal::Decimal(const SmallInt value){
    this->InitializeFromInteger<SmallInt>(value);
}

constexpr Decimal::Decimal(const Int value){
    this->InitializeFromInteger<Int>(value);
}

constexpr Decimal::Decimal(const BigInt value){
    this->InitializeFromInteger<BigInt>(value);
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

constexpr byte_t Decimal::CreateSignAndFractionByte(
    const bool isPositive,
    const fraction_index_t fractionIndex
) {
    return (isPositive << 7) | (fractionIndex & 0x7F);
}

constexpr Decimal::DataBuffer Decimal::Pack(const AdditionDigitsBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
    DataBuffer result;
    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

    for (Int i = 0; i < digits.Size(); i += 2) {
        const Int high = digits[i];
        const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
        result.Push((high << 4) | low);
    }

    return result;
}

constexpr Decimal::DataBuffer Decimal::Pack(const MultiplicationDigitsBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
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

    const auto fractionIndex = leftFractionIndex > rightFractionIndex
                                   ? leftFractionIndex
                                   : rightFractionIndex;

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    //both same sign, add them and use sign afterwards
    if (leftSign == rightSign)
        return Decimal::Add(leftCopy, rightCopy, fractionIndex, leftSign);

    //else they have different signs
    //so subtract them
    return (Decimal::IsGreaterMagnitude(leftCopy, rightCopy))
               ? Decimal::Subtract(leftCopy, rightCopy, fractionIndex, leftSign)
               : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, rightSign);
}

constexpr Decimal operator-(const Decimal &left, const Decimal &right){
    const bool leftSign = left.IsPositive();
    const bool rightSign = right.IsPositive();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();
    const auto fractionIndex = leftFractionIndex > rightFractionIndex
                                   ? leftFractionIndex
                                   : rightFractionIndex;

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    if (leftSign == rightSign)
        return (Decimal::IsGreaterMagnitude(leftCopy, rightCopy))
                   ? Decimal::Subtract(leftCopy, rightCopy, fractionIndex, leftSign)
                   : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, !leftSign);

    return Decimal::Add(leftCopy, rightCopy, fractionIndex, leftSign);
}

constexpr Decimal operator*(const Decimal &left, const Decimal &right){
    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    auto leftCopy = left.Data();
    auto rightCopy = right.Data();

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    fraction_index_t fractionIndex = leftFractionIndex + rightFractionIndex;

    return Decimal::Multiply(
        leftCopy,
        rightCopy,
        fractionIndex,
        left.IsPositive() == right.IsPositive()
    );
}

constexpr Decimal operator/(const Decimal& left, const Decimal& right){
    return Decimal();
    // Decimal left1(left);
    // return left1 /= right;
}

constexpr Int Decimal::Size(const Int precision){
    return precision / 2 + 1 + 1; //+1 for sign and fraction index, +1 for alignment
}

constexpr bool operator==(const Decimal& left, const Decimal& right){
    const bool leftSign = left.IsPositive();
    const bool rightSign = right.IsPositive();

    const auto& leftData = left.Data();
    const auto& rightData = right.Data();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    auto leftCopy = leftData;
    auto rightCopy = rightData;

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    if (leftSign != rightSign)
        return false;

    return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) == Decimal::ComparisonResult::Equal;
}

constexpr bool operator>=(const Decimal& left, const Decimal& right){
    const bool leftSign = left.IsPositive();
    const bool rightSign = right.IsPositive();

    const auto& leftData = left.Data();
    const auto& rightData = right.Data();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    auto leftCopy = leftData;
    auto rightCopy = rightData;

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    if (leftSign > rightSign) return true;
    if (leftSign < rightSign) return false;

    return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) >= Decimal::ComparisonResult::Equal;
}

constexpr bool operator>(const Decimal& left, const Decimal& right){
    const bool leftSign = left.IsPositive();
    const bool rightSign = right.IsPositive();

    const auto& leftData = left.Data();
    const auto& rightData = right.Data();

    auto leftFractionIndex = left.GetFractionIndex();
    auto rightFractionIndex = right.GetFractionIndex();

    auto leftCopy = leftData;
    auto rightCopy = rightData;

    Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
    Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

    if (leftSign > rightSign) return true;
    if (leftSign < rightSign) return false;

    return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) == Decimal::ComparisonResult::Greater;
}

constexpr bool operator<(const Decimal& left, const Decimal& right){
    return !(left >= right);
}

constexpr bool operator<=(const Decimal& left, const Decimal& right){
    return !(left > right);
}

constexpr Decimal& Decimal::operator=(const TinyInt right){
    *this = Decimal(right);
    return *this;
}

constexpr Decimal& Decimal::operator=(const SmallInt right){
    *this = Decimal(right);
    return *this;
}

constexpr Decimal& Decimal::operator=(const Int right){
    *this = Decimal(right);
    return *this;
}

constexpr Decimal& Decimal::operator=(const BigInt right){
    *this = Decimal(right);
    return *this;
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

constexpr bool Decimal::IsGreaterMagnitude(const DataBuffer& left, const DataBuffer& right){
    for (Int i = 1; i < left.Size(); i++) {
        if (left[i] == right[i]) continue;
        return left[i] > right[i];
    }

    return false;
}

constexpr Decimal::AdditionDigitsBuffer Decimal::Unpack(const DataBuffer& bytes){
    AdditionDigitsBuffer digits;

    for (int i = 1; i < bytes.Size(); i++) {
        digits.Push((bytes[i] >> 4) & 0x0F);
        digits.Push(bytes[i] & 0x0F);
    }

    return digits;
}

constexpr Decimal::MultiplicationDigitsBuffer Decimal::MultiplyDigits(const AdditionDigitsBuffer& leftDigits, const AdditionDigitsBuffer& rightDigits){
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

constexpr Decimal::ComparisonResult Decimal::CompareDecimalsWithoutSign(
    const DataBuffer& leftData,
    const DataBuffer& rightData
){
    for (int i = 1; i < leftData.Size(); i++){
        const auto leftByte = leftData[i];
        const auto rightByte = rightData[i];

        const auto leftHigh = (leftByte >> 4) & 0x0F;
        const auto leftLow = leftByte & 0x0F;

        const auto rightHigh = (rightByte >> 4) & 0x0F;
        const auto rightLow = rightByte & 0x0F;

        if (leftHigh > rightHigh) return ComparisonResult::Greater;
        if (leftHigh < rightHigh) return ComparisonResult::Less;
        if (leftLow > rightLow) return ComparisonResult::Greater;
        if (leftLow < rightLow) return ComparisonResult::Less;
    }

    return ComparisonResult::Equal;
}

constexpr Decimal Decimal::Add(
    const DataBuffer& left, const DataBuffer& right,
    const fraction_index_t fractionIndex,
    const bool isPositive
){
    DataBuffer result;
    Int carry = 0;

    for (auto i = left.Size() - 1; i > 0; i--) {
        // Extract digits from packed format
        const auto leftHigh = (left[i] >> 4) & 0x0F;
        const auto leftLow = left[i] & 0x0F;

        const auto rightHigh = (right[i] >> 4) & 0x0F;
        const auto rightLow = right[i] & 0x0F;

        // Add low digits
        int sumLow = leftLow + rightLow + carry;
        carry = sumLow / 10;
        sumLow %= 10;

        // Add high digits
        int sumHigh = leftHigh + rightHigh + carry;
        carry = sumHigh / 10;
        sumHigh %= 10;

        // Pack result back into byte
        byte_t packedByte = (sumHigh << 4) | sumLow;
        result.Push(packedByte);
    }

    if (carry > 0) result.Push(carry);

    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));
    std::ranges::reverse(result);

    return Decimal(result.Data(), result.Size());
}

constexpr void Decimal::PadFractionalParts(
    DataBuffer& left, DataBuffer& right,
    const fraction_index_t leftFractionIndex,
    const fraction_index_t rightFractionIndex
){
    const int leftFracDigits = left.Size() * 2 - leftFractionIndex;
    const int rightFracDigits = right.Size() * 2 - rightFractionIndex;
    const int maxFracDigits = std::max(leftFracDigits, rightFracDigits);

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
    DataBuffer& left, DataBuffer& right,
    fraction_index_t &leftFractionIndex,
    fraction_index_t &rightFractionIndex
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
    const DataBuffer& left, const DataBuffer& right,
    const fraction_index_t fractionIndex,
    const bool isPositive
){
    DataBuffer result;
    Int carry = 0;

    for (Int i = left.Size() - 1; i > 0; i--) {
        // Extract digits from packed format
        const auto leftHigh = (left[i] >> 4) & 0x0F;
        const auto leftLow = left[i] & 0x0F;

        const auto rightHigh = (right[i] >> 4) & 0x0F;
        const auto rightLow = right[i] & 0x0F;

        // Add low digits
        Int sumLow = leftLow - rightLow + carry;
        carry = sumLow / 10;
        sumLow %= 10;

        // Add high digits
        Int sumHigh = leftHigh - rightHigh + carry;
        carry = sumHigh / 10;
        sumHigh %= 10;

        // Pack result back into byte
        byte_t packedByte = (sumHigh << 4) | sumLow;
        result.Push(packedByte);
    }

    if (carry > 0)  result.Push(carry);
    result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

    std::ranges::reverse(result);

    return Decimal(result.Data(), result.Size());
}

constexpr Decimal Decimal::Multiply(
    const DataBuffer& left, const DataBuffer& right,
    fraction_index_t &fractionIndex,
    const bool isPositive
){
    const auto leftDigits  = Decimal::Unpack(left);
    const auto rightDigits = Decimal::Unpack(right);

    auto productDigits = Decimal::MultiplyDigits(leftDigits, rightDigits);

    Decimal::TrimLeadingZeros(productDigits, fractionIndex);
    Decimal::TrimTrailingZeros(productDigits, fractionIndex);
    Decimal::PadDecimalParts(productDigits, fractionIndex);

    const auto packed = Decimal::Pack(productDigits, isPositive, fractionIndex);
    return Decimal(packed.Data(), packed.Size());
}

constexpr Decimal Decimal::Divide(
    const std::vector<byte_t> &left,
    const std::vector<byte_t> &right,
    fraction_index_t &fractionIndex,
    const bool isPositive
){
    return Decimal();
}

constexpr std::ostream & operator<<(std::ostream &os, const Decimal &decimal){
    const auto buffer = decimal.ToBufferString();
    os.write(buffer.Data(), buffer.Size());
    return os;
}
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

