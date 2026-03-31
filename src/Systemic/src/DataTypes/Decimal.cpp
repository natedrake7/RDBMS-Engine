#include "../../include/DataTypes/Decimal.h"
#include <algorithm>
#include <iostream>

#include "DataTypes/StringView.h"

namespace DataTypes {
    template <typename T>
    void Decimal::InitializeFromInteger(const T value){
        static_assert(std::is_integral_v<T>, "Decimal::InitializeFromInteger: T must be an integer");

        const auto isPositive = value >= 0;
        auto absValue = std::abs(static_cast<BigInt>(value));

        TempBuffer digits;
        Int counter = 0;
        if (absValue == 0)
            digits.Push(DECIMAL_ZERO);

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

    Decimal::TempBuffer Decimal::Unpack(const DataBuffer& bytes){
        TempBuffer digits;

        for (int i = 1; i < bytes.Size(); i++) {
            digits.Push((bytes[i] >> 4) & 0x0F);
            digits.Push(bytes[i] & 0x0F);
        }

        return digits;
    }

    Decimal::MultiplicationBuffer Decimal::MultiplyDigits(const TempBuffer& leftDigits, const TempBuffer& rightDigits){
        MultiplicationBuffer result;
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

    Decimal::DataBuffer Decimal::Pack(const TempBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
        DataBuffer result;
        result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        for (Int i = 0; i < digits.Size(); i += 2) {
            const Int high = digits[i];
            const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
            result.Push((high << 4) | low);
        }

        return result;
    }

    Decimal::DataBuffer Decimal::Pack(const MultiplicationBuffer& digits, const bool isPositive, const fraction_index_t fractionIndex){
        DataBuffer result;
        result.Push(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        for (Int i = 0; i < digits.Size(); i += 2) {
            const Int high = digits[i];
            const Int low  = (i + 1 < digits.Size()) ? digits[i+1] : 0;
            result.Push((high << 4) | low);
        }

        return result;
    }

    // fraction_index_t Decimal::GetFractionIndex(const StringView& value){
    //     value
    //     const auto decimalPos = static_cast<int>(value.find('.'));
    //
    //     return (decimalPos != std::string::npos)
    //                ? decimalPos
    //                : 0;
    // }

    fraction_index_t Decimal::DetermineResultFractionIndex(
        const fraction_index_t leftFractionIndex,
        const fraction_index_t rightFractionIndex){
        return std::max(leftFractionIndex, rightFractionIndex);
    }

    Decimal::ComparisonResult Decimal::CompareDecimalsWithoutSign(
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

    Decimal Decimal::Add(
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

    void Decimal::PadFractionalParts(
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

    void Decimal::PadNonFractionalParts(
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

    Decimal Decimal::Subtract(
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

    Decimal Decimal::Multiply(
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

    Decimal Decimal::Divide(
        const std::vector<byte_t> &left,
        const std::vector<byte_t> &right,
        fraction_index_t &fractionIndex,
        const bool isPositive
    ){
    }

    void Decimal::TrimLeadingZeros(MultiplicationBuffer& digits, fraction_index_t &fractionIndex){
        auto leadingZeros = 0;
        while (leadingZeros < fractionIndex && digits[leadingZeros] == 0)
            leadingZeros++;

        if (leadingZeros == 0) return;

        digits.Remove(0, leadingZeros);
        fractionIndex -= static_cast<fraction_index_t>(leadingZeros);
    }

    void Decimal::TrimTrailingZeros(MultiplicationBuffer& digits, const fraction_index_t fractionIndex){
        for (auto i = digits.Size() - 1; i > fractionIndex; i--) {
            if (digits[i] != 0) break;
            digits.Pop();
        }
    }

    void Decimal::PadDecimalParts(MultiplicationBuffer& digits, fraction_index_t& fractionIndex){
        if (fractionIndex % 2 != 0) {
            digits.Insert(0, 0);
            fractionIndex++;
        }

        const auto fractionalPart = digits.Size() - fractionIndex;
        if (fractionalPart % 2 != 0) digits.Push(DECIMAL_ZERO);
    }

    byte_t Decimal::CreateSignAndFractionByte(
        const bool isPositive,
        const fraction_index_t fractionIndex
    ) {
        return (isPositive << 7) | (fractionIndex & 0x7F);
    }

    bool Decimal::IsGreaterMagnitude(const DataBuffer& left, const DataBuffer& right){
        for (size_t i = 1; i < left.Size(); i++) {
            if (left[i] == right[i]) continue;
            return left[i] > right[i];
        }

        //equality (return 0)
        return false;
    }

    Decimal::Decimal(): _data(0) {}

    Decimal::Decimal(const StringView& value){
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

    Decimal::Decimal(const byte_t* data, const Int dataSize){
        this->_data.SetData(data, dataSize);
    }

    // Decimal::Decimal(const std::vector<byte_t> &value){
    //     // this->bytes = value;
    // }

    Decimal::Decimal(const bool value){
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

    Decimal::Decimal(const TinyInt value){
        this->InitializeFromInteger<TinyInt>(value);
    }

    Decimal::Decimal(const SmallInt value){
        this->InitializeFromInteger<SmallInt>(value);
    }

    Decimal::Decimal(const Int value){
        this->InitializeFromInteger<Int>(value);
    }

    Decimal::Decimal(const BigInt value){
        this->InitializeFromInteger<BigInt>(value);
    }

    Decimal::~Decimal() = default;

    bool Decimal::IsPositive() const { return ( this->_data[DECIMAL_HEADER_INDEX] >> 7 ) & 0x01; }

    fraction_index_t Decimal::GetFractionIndex() const { return static_cast<fraction_index_t>(this->_data[DECIMAL_HEADER_INDEX] & 0x7F); }

    String Decimal::ToString(const ::Memory::IAllocator* allocator) const{
        const auto isPositive = this->IsPositive();

        String result(allocator, this->_data.Size());

        if (!isPositive) result[0] = '-';

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex() + !isPositive;

        Int strCounter = !isPositive;
        for (int i = DECIMAL_DIGITS_START_INDEX; i < this->_data.Size(); i++){
            if (strCounter == fractionIndex){
                result[strCounter] = '.';

                i--;
                strCounter++;

                continue;
            }

            result[strCounter] = static_cast<char>((this->_data[i] >> 4) & 0x0F);
            strCounter++;
        }

        if (result.Last() == '0')
            result.Pop();

        return result;
    }

    const byte_t* Decimal::GetRawData() const { return this->_data.Data(); }

    Int Decimal::GetRawDataSize() const { return this->_data.Size(); }

    const Decimal::DataBuffer& Decimal::Data() const { return this->_data; }

    Int Decimal::Size(const Int precision){
        return precision / 2 + 1 + 1; //+1 for sign and fraction index, +1 for alignment
    }

    double Decimal::ToDouble() const{
        // return std::stod(this->ToString());
    }

    std::ostream & operator<<(std::ostream &os, const Decimal &decimal){
        // os << decimal.ToString();
        return os;
    }

    Decimal operator+(const Decimal &left, const Decimal &right){
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

    Decimal operator-(const Decimal &left, const Decimal &right){
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

    Decimal operator*(const Decimal &left, const Decimal &right){
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

    bool operator==(const Decimal& left, const Decimal& right){
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

    bool operator>=(const Decimal& left, const Decimal& right){
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

    bool operator>(const Decimal& left, const Decimal& right){
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

    bool operator<(const Decimal& left, const Decimal& right){
        return !(left >= right);
    }

    bool operator<=(const Decimal& left, const Decimal& right){
        return !(left > right);
    }

    Decimal& Decimal::operator=(const TinyInt right){
        *this = Decimal(right);
        return *this;
    }

    Decimal& Decimal::operator=(const SmallInt right){
        *this = Decimal(right);
        return *this;
    }

    Decimal& Decimal::operator=(const Int right){
        *this = Decimal(right);
        return *this;
    }

    Decimal& Decimal::operator=(const BigInt right){
        *this = Decimal(right);
        return *this;
    }
}


