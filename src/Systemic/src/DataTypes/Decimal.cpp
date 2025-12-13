#include "../../include/DataTypes/Decimal.h"
#include <algorithm>
#include <iostream>

namespace DataTypes {
    Decimal::Decimal() = default;

    Decimal::Decimal(const std::string& value)
    {
        if (value.empty())
            return;

        string copiedValue(value);

        //remove sign
        const bool isPositive = copiedValue.front() != '-';

        if (!isPositive || copiedValue.front() == '+')
            copiedValue.erase(0, 1);

        for (const auto& ch : copiedValue) {
            if (ch != '0')
                break;

            copiedValue.erase(0, 1);
        }

        auto fractionIndex = Decimal::GetFractionIndex(copiedValue);

        //invalid string
        if (fractionIndex != 0) {
            copiedValue.erase(fractionIndex , 1);
            const auto fractionalDigits = copiedValue.size() - fractionIndex;

            const auto integerDigits = copiedValue.size() - fractionalDigits;

            if (fractionalDigits % 2 != 0)
                copiedValue.push_back('0');

            if (integerDigits % 2 != 0) {
                copiedValue.insert(copiedValue.begin(), '0');
                fractionIndex++;
            }
        }

        const byte_t signAndFractionPoint = (isPositive << 7) | (fractionIndex & 0x7F);

        this->bytes.push_back(signAndFractionPoint);


        for (int i = 0; i < copiedValue.size(); i+= 2)
        {
            byte_t val = 0;

            val |= (copiedValue[i] - '0') << 4;

            if (i + 1 < copiedValue.size()) 
                val |= (copiedValue[i + 1] - '0');

            this->bytes.push_back(val);
        }
    }

    Decimal::Decimal(const bool &value){
        //boolean is always positive
        constexpr auto isPositive = true;

        //fraction index is always at a fixed position
        constexpr fraction_index_t fractionIndex = 2;

        constexpr byte_t signAndFractionPoint = (isPositive << 7) | (fractionIndex & 0x7F);

        this->bytes.push_back(signAndFractionPoint);

        byte_t val = 0;

        val |= (value ? 1 : 0);

        this->bytes.push_back(val);

        //add alignment byte
        this->bytes.push_back(0x00);
    }

    Decimal::Decimal(const int8_t &value){
        this->InitializeFromInteger<int8_t>(value);
    }

    Decimal::Decimal(const int16_t &value){
        this->InitializeFromInteger<int16_t>(value);
    }

    Decimal::Decimal(const int32_t &value){
        this->InitializeFromInteger<int32_t>(value);
    }

    Decimal::Decimal(const int64_t &value){
        this->InitializeFromInteger<int64_t>(value);
    }

    Decimal::Decimal(const byte_t* data, const int& dataSize)
    {
        this->bytes = std::vector(data, data + dataSize);
    }

    Decimal::Decimal(const vector<byte_t> &value)
    {
        this->bytes = value;
    }


    Decimal::~Decimal() = default;

    bool Decimal::IsPositive() const { return ( this->bytes.at(0) >> 7 ) & 0x01; }

    fraction_index_t Decimal::GetFractionIndex() const { return static_cast<fraction_index_t>(this->bytes.at(0) & 0x7F); }

    string Decimal::ToString() const
    {
        const bool isPositive = this->IsPositive();
        
        string result = ( isPositive ? "" : "-" );

        for (int i = 1; i < this->bytes.size(); i++)
        {
            const auto& byte = this->bytes.at(i);

            result += to_string((byte >> 4) & 0x0F);
            result += to_string(byte & 0x0F);
        }

        // if (result.back() == '0')
        //     result.pop_back();

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex() + ( isPositive ? 0 : 1);

        result.insert(fractionIndex,  ".");

        return result;
    }

    const byte_t* Decimal::GetRawData() const { return this->bytes.data(); }

    int Decimal::GetRawDataSize() const { return this->bytes.size(); }

    const vector<byte_t>& Decimal::GetData() const { return this->bytes; }

    byte_t Decimal::CreateSignAndFractionByte(
        const bool& isPositive,
        const fraction_index_t& fractionIndex) {
        return (isPositive << 7) | (fractionIndex & 0x7F);
    }

    Decimal operator+(const Decimal &left, const Decimal &right)
    {
        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        fraction_index_t leftFractionIndex = left.GetFractionIndex();
        fraction_index_t rightFractionIndex = right.GetFractionIndex();

        const auto fractionIndex = leftFractionIndex > rightFractionIndex
                            ? leftFractionIndex
                            : rightFractionIndex;

        auto leftCopy = leftData;
        auto rightCopy = rightData;

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

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        auto leftFractionIndex = left.GetFractionIndex();
        auto rightFractionIndex = right.GetFractionIndex();
        const auto fractionIndex = leftFractionIndex > rightFractionIndex
                        ? leftFractionIndex
                        : rightFractionIndex;

        auto leftCopy = leftData;
        auto rightCopy = rightData;

        Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
        Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

        if (leftSign == rightSign)
            return (Decimal::IsGreaterMagnitude(leftCopy, rightCopy))
                ? Decimal::Subtract(leftCopy, rightCopy, fractionIndex, leftSign)
                : Decimal::Subtract(rightCopy, leftCopy, fractionIndex, !leftSign);

        return Decimal::Add(leftCopy, rightCopy, fractionIndex, leftSign);
    }

    Decimal operator*(const Decimal &left, const Decimal &right){
        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        auto leftFractionIndex = left.GetFractionIndex();
        auto rightFractionIndex = right.GetFractionIndex();

        auto leftCopy = leftData;
        auto rightCopy = rightData;

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

    bool operator==(const Decimal& left, const Decimal& right)
    {
        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        auto leftFractionIndex = left.GetFractionIndex();
        auto rightFractionIndex = right.GetFractionIndex();

        auto leftCopy = leftData;
        auto rightCopy = rightData;

        Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
        Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

        if (leftSign != rightSign)
            return false;

        return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) == 0;
    }

    bool operator>=(const Decimal& left, const Decimal& right)
    {
        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        auto leftFractionIndex = left.GetFractionIndex();
        auto rightFractionIndex = right.GetFractionIndex();

        auto leftCopy = leftData;
        auto rightCopy = rightData;

        Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
        Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

        if (leftSign > rightSign)
            return true;

        if (leftSign < rightSign)
            return false;

        return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) >= 0;
    }

    bool operator>(const Decimal& left, const Decimal& right)
    {
        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        auto leftFractionIndex = left.GetFractionIndex();
        auto rightFractionIndex = right.GetFractionIndex();

        auto leftCopy = leftData;
        auto rightCopy = rightData;

        Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
        Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

        if (leftSign > rightSign)
            return true;

        if (leftSign < rightSign)
            return false;

        return Decimal::CompareDecimalsWithoutSign(leftCopy, rightCopy) > 0;
    }

    bool operator<(const Decimal& left, const Decimal& right)
    {
        return !(left >= right);
    }

    bool operator<=(const Decimal& left, const Decimal& right)
    {
        return !(left > right);
    }

    int Decimal::CompareDecimalsWithoutSign(const std::vector<byte_t>& leftData, const std::vector<byte_t>& rightData)
    {
        for (int i = 1; i < leftData.size(); i++)
        {
            const auto leftByte = leftData.at(i);
            const auto rightByte = rightData.at(i);

            const auto leftHigh = (leftByte >> 4) & 0x0F;
            const auto leftLow = leftByte & 0x0F;

            const auto rightHigh = (rightByte >> 4) & 0x0F;
            const auto rightLow = rightByte & 0x0F;

            if (leftHigh > rightHigh)
                return 1;

            if (leftHigh < rightHigh)
                return -1;

            if (leftLow > rightLow)
                return 1;

            if (leftLow < rightLow)
                return -1;
        }

        return 0;
    }

    std::vector<int> Decimal::Unpack(const vector<byte_t> &bytes){
        std::vector<int> digits;

        for (int i = 1; i < bytes.size(); i++) {
            digits.push_back((bytes[i] >> 4) & 0x0F);
            digits.push_back(bytes[i] & 0x0F);
        }

        return digits;
    }

    std::vector<int> Decimal::MultiplyDigits(const std::vector<int> &leftDigits, const std::vector<int> &rightDigits){
        std::vector<int> result(leftDigits.size() + rightDigits.size(), 0);

        for (int i = leftDigits.size() - 1; i >= 0; --i) {
            for (int j = rightDigits.size() - 1; j >= 0; --j) {
                const int pos = i + j + 1;
                result[pos] += leftDigits[i] * rightDigits[j];
            }
        }

        // Step 3: fix carries
        for (int k = result.size() - 1; k > 0; k--) {
            result[k-1] += result[k] / 10;
            result[k] %= 10;
        }

        return result;
    }

    std::vector<byte_t> Decimal::Pack(
        const std::vector<int> &digits,
        const bool& isPositive,
        const fraction_index_t& fractionIndex
    ){
        std::vector<byte_t> out;
        out.push_back(Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        for (int i = 0; i < digits.size(); i += 2) {
            const int high = digits[i];
            const int low  = (i+1 < digits.size()) ? digits[i+1] : 0;
            out.push_back((high << 4) | low);
        }
        return out;
    }

    fraction_index_t Decimal::GetFractionIndex(const string& value)
    {
        const int decimalPos = static_cast<int>(value.find('.'));

        return (decimalPos != string::npos) 
                            ? decimalPos 
                            : 0;
    }

    fraction_index_t Decimal::DetermineResultFractionIndex(
        const fraction_index_t &leftFractionIndex,
        const fraction_index_t &rightFractionIndex){
        return std::max(leftFractionIndex, rightFractionIndex);
    }

    Decimal Decimal::Add(
        const std::vector<byte_t> &left,
        const std::vector<byte_t> &right,
        const fraction_index_t &fractionIndex,
        const bool &isPositive){
        std::vector<byte_t> result;
        int carry = 0;

        for (int i = left.size() - 1; i > 0; i--) {
            // Extract digits from packed format
            const int leftHigh = (left[i] >> 4) & 0x0F;
            const int leftLow = left[i] & 0x0F;

            const int rightHigh = (right[i] >> 4) & 0x0F;
            const int rightLow = right[i] & 0x0F;

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
            result.push_back(packedByte);
        }

        if (carry > 0)
            result.push_back(carry);

        ranges::reverse(result);
        result.insert(result.begin(), Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        return Decimal(result);
    }

    void Decimal::PadFractionalParts(
        std::vector<byte_t> &left,
        std::vector<byte_t> &right,
        fraction_index_t &leftFractionIndex,
        fraction_index_t &rightFractionIndex
    ){
        const int leftFracDigits = left.size() * 2 - leftFractionIndex;
        const int rightFracDigits = right.size() * 2 - rightFractionIndex;
        const int maxFracDigits = max(leftFracDigits, rightFracDigits);

        if (leftFracDigits < maxFracDigits) {
            const int digitsToAdd = maxFracDigits - leftFracDigits;
            const int bytesToAdd = (digitsToAdd + 1) / 2;

            left.insert(left.end(), bytesToAdd, 0);
        }

        if (rightFracDigits < maxFracDigits) {
            const int digitsToAdd = maxFracDigits - rightFracDigits;
            const int bytesToAdd = (digitsToAdd + 1) / 2;

            right.insert(right.end(), bytesToAdd, 0);
        }
    }

    void Decimal::PadNonFractionalParts(
        std::vector<byte_t> &left,
        std::vector<byte_t> &right,
        fraction_index_t &leftFractionIndex,
        fraction_index_t &rightFractionIndex){

        const int leftNonFracSize = leftFractionIndex / 2;
        const int rightNonFracSize = rightFractionIndex / 2;
        const int maxNonFracSize = max(leftNonFracSize, rightNonFracSize);

        // Pad left with leading zeros if needed
        if (leftNonFracSize < maxNonFracSize) {
            left.insert(left.begin() + 1, maxNonFracSize - leftNonFracSize, 0x00);
            leftFractionIndex += static_cast<fraction_index_t>((maxNonFracSize - leftNonFracSize) * 2);
        }

        // Pad right with leading zeros if needed
        if (rightNonFracSize < maxNonFracSize) {
            right.insert(right.begin() + 1, maxNonFracSize - rightNonFracSize, 0x00);
            rightFractionIndex += static_cast<fraction_index_t>((maxNonFracSize - rightNonFracSize) * 2);
        }
    }

    Decimal Decimal::Subtract(
        const std::vector<byte_t> &left,
        const std::vector<byte_t> &right,
        const fraction_index_t& fractionIndex,
        const bool &isPositive
    ){
        std::vector<byte_t> result;
        int carry = 0;

        for (int i = left.size() - 1; i > 0; i--) {
            // Extract digits from packed format
            const int leftHigh = (left[i] >> 4) & 0x0F;
            const int leftLow = left[i] & 0x0F;

            const int rightHigh = (right[i] >> 4) & 0x0F;
            const int rightLow = right[i] & 0x0F;

            // Add low digits
            int sumLow = leftLow - rightLow + carry;
            carry = sumLow / 10;
            sumLow %= 10;

            // Add high digits
            int sumHigh = leftHigh - rightHigh + carry;
            carry = sumHigh / 10;
            sumHigh %= 10;

            // Pack result back into byte
            byte_t packedByte = (sumHigh << 4) | sumLow;
            result.push_back(packedByte);
        }

        if (carry > 0)
            result.push_back(carry);

        ranges::reverse(result);
        result.insert(result.begin(), Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        return Decimal(result);
    }

    Decimal Decimal::Multiply(
        const std::vector<byte_t> &left,
        const std::vector<byte_t> &right,
        fraction_index_t &fractionIndex,
        const bool &isPositive
    ){
        const auto leftDigits  = Decimal::Unpack(left);
        const auto rightDigits = Decimal::Unpack(right);

        auto productDigits = Decimal::MultiplyDigits(leftDigits, rightDigits);

        Decimal::TrimLeadingZeros(productDigits, fractionIndex);
        Decimal::TrimTrailingZeros(productDigits, fractionIndex);
        Decimal::PadDecimalParts(productDigits, fractionIndex);

        const auto packed = Decimal::Pack(productDigits, isPositive, fractionIndex);

        return Decimal(packed);
    }

    Decimal Decimal::Divide(
        const std::vector<byte_t> &left,
        const std::vector<byte_t> &right,
        fraction_index_t &fractionIndex,
        const bool &isPositive){
    }

    void Decimal::TrimLeadingZeros(
        std::vector<int> &digits,
        fraction_index_t &fractionIndex){

        auto leadingZeros = 0;
        while (leadingZeros < fractionIndex && digits[leadingZeros] == 0)
            leadingZeros++;

        if (leadingZeros == 0)
            return;

        digits.erase(digits.begin(), digits.begin() + leadingZeros);
        fractionIndex -= static_cast<fraction_index_t>(leadingZeros);
    }

    void Decimal::TrimTrailingZeros(
        std::vector<int> &digits,
        const fraction_index_t &fractionIndex
    ){
        for (int i = digits.size() - 1; i > fractionIndex; i--) {
            if (digits[i] != 0)
                break;

            digits.pop_back();
        }
    }

    void Decimal::PadDecimalParts(
        std::vector<int> &digits,
        fraction_index_t &fractionIndex){

        if (fractionIndex % 2 != 0) {
            digits.insert(digits.begin(), 0);
            fractionIndex++;
        }

        const int fractionalPart = digits.size() - fractionIndex;
        if (fractionalPart % 2 != 0)
            digits.push_back(0);
    }

    bool Decimal::IsGreaterMagnitude(const std::vector<byte_t> &left, const std::vector<byte_t> &right){
        for (size_t i = 1; i < left.size(); i++) {
            if (left[i] == right[i])
                continue;

            return left[i] > right[i];
        }

        //equality (return 0)
        return false;
    }

    ostream & operator<<(ostream &os, const Decimal &decimal){
        os << decimal.ToString();
        return os;
    }

    template <typename T>
    void Decimal::InitializeFromInteger(const T &value){
        static_assert(is_integral_v<T>, "Decimal::InitializeFromInteger: T must be an integer");

        const auto isPositive = value >= 0;

        auto absValue = std::abs(static_cast<int64_t>(value));

        std::vector<int> digits;

        if (absValue == 0)
            digits.push_back(0);

        while (absValue > 0) {
            digits.push_back(absValue % 10);
            absValue /= 10;
        }

        ranges::reverse(digits);

        digits.push_back(0x00);
        digits.push_back(0x00);

        fraction_index_t fractionIndex = digits.size() - 2;

        if (fractionIndex % 2 != 0) {
            digits.insert(digits.begin(), 0);
            fractionIndex++;
        }

        this->bytes = Decimal::Pack(digits, isPositive, fractionIndex);
    }

    int Decimal::Size(const int &precision){
        return precision / 2 + 1 + 1; //+1 for sign and fraction index, +1 for alignment
    }

    Decimal & Decimal::operator=(const int8_t &right){
        *this = Decimal(right);
        return *this;
    }

    Decimal & Decimal::operator=(const int16_t &right){
        *this = Decimal(right);
        return *this;
    }

    Decimal & Decimal::operator=(const int32_t &right){
        *this = Decimal(right);
        return *this;
    }

    Decimal & Decimal::operator=(const int64_t &right){
        *this = Decimal(right);
        return *this;
    }
}


