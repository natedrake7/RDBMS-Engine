#include "Decimal.h"

#include "../../Functions/StringFunctions.h"

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

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex(copiedValue);

        const Constants::byte signAndFractionPoint = (isPositive << 7) | (fractionIndex & 0x7F);

        this->bytes.push_back(signAndFractionPoint);

        //invalid string
        if (fractionIndex != 0) {
            copiedValue.erase(fractionIndex , 1);
            const auto fractionalDigits = copiedValue.size() - fractionIndex;

            if (fractionalDigits % 2 != 0)
                copiedValue.push_back('0');
        }

        for (int i = 0; i < copiedValue.size(); i+= 2)
        {
            Constants::byte val = 0;

            val |= (copiedValue[i] - '0') << 4;

            if (i + 1 < copiedValue.size()) 
                val |= (copiedValue[i + 1] - '0');

            this->bytes.push_back(val);
        }
    }

    Decimal::Decimal(const Constants::byte* data, const int& dataSize)
    {
        this->bytes = std::vector(data, data + dataSize);
    }

    Decimal::Decimal(const vector<Constants::byte> &value)
    {
        this->bytes = value;
    }

    Decimal::~Decimal() = default;

    bool Decimal::IsPositive() const { return ( this->bytes.at(0) >> 7 ) & 0x01; }

    fraction_index_t Decimal::GetFractionIndex() const { return static_cast<Constants::fraction_index_t>(this->bytes.at(0) & 0x7F); }

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

        if (result.back() == '0')
            result.pop_back();

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex() + ( isPositive ? 0 : 1);

        result.insert(fractionIndex,  ".");

        return result;
    }

    const Constants::byte* Decimal::GetRawData() const { return this->bytes.data(); }

    int Decimal::GetRawDataSize() const { return this->bytes.size(); }

    const vector<Constants::byte>& Decimal::GetData() const { return this->bytes; }

    Constants::byte Decimal::CreateSignAndFractionByte(
        const bool& isPositive,
        const fraction_index_t& fractionIndex) {
        return (isPositive << 7) | (fractionIndex & 0x7F);
    }

    Decimal operator+(const Decimal &left, const Decimal &right)
    {
        vector<Constants::byte> result;

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        const fraction_index_t leftFractionIndex = left.GetFractionIndex();
        const fraction_index_t rightFractionIndex = right.GetFractionIndex();

        const auto fractionIndex = leftFractionIndex > rightFractionIndex ? leftFractionIndex : rightFractionIndex;

        //both same sign, add them and use sign afterwards
        if (leftSign == rightSign)
            return Decimal::Add(left.GetData(), right.GetData(), leftFractionIndex, rightFractionIndex, fractionIndex, leftSign);

        //else they have different signs
        //so subtract them
        if (Decimal::IsGreaterMagnitude(leftData, rightData))
            return Decimal::Subtract(leftData, rightData, fractionIndex, leftSign);

        return Decimal::Subtract(rightData, leftData, fractionIndex, rightSign);
    }

    bool operator==(const Decimal& left, const Decimal& right)
    {
        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool hasLeftDecimalGreaterLength = leftData.size() > rightData.size();
        const int minSize = hasLeftDecimalGreaterLength
                            ? rightData.size() 
                            : leftData.size();

        for(int i = 0; i < minSize; i++)
            if(((leftData[i] >> 4) & 0x0F ) != ((rightData[i] >> 4) & 0x0F)
                || (leftData[i] & 0x0F) != (rightData[i] & 0x0F))
                return false;

        return hasLeftDecimalGreaterLength 
                ? CompareDecimals(leftData, rightData.size()) == 0
                : CompareDecimals(rightData, leftData.size()) == 0;
    }

    bool operator>=(const Decimal& left, const Decimal& right)
    {
        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool hasLeftDecimalGreaterLength = leftData.size() > rightData.size();
        const bool hasRightDecimalGreaterLength = rightData.size() > leftData.size();

        const int minSize = hasLeftDecimalGreaterLength
                            ? rightData.size() 
                            : leftData.size();

        for(int i = 0; i < minSize; i++)
        {
            if(((leftData[i] >> 4) & 0x0F ) > ((rightData[i] >> 4) & 0x0F)
                || (leftData[i] & 0x0F) > (rightData[i] & 0x0F))
                return true;
        }

        return hasLeftDecimalGreaterLength 
                ? CompareDecimals(leftData, rightData.size()) >= 0
                : CompareDecimals(rightData, leftData.size()) == 0;
    }

    bool operator>(const Decimal& left, const Decimal& right)
    {
        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool hasLeftDecimalGreaterLength = leftData.size() > rightData.size();
        const bool hasRightDecimalGreaterLength = rightData.size() > leftData.size();

        const int minSize = hasLeftDecimalGreaterLength
                            ? rightData.size() 
                            : leftData.size();

        for(int i = 0; i < minSize; i++)
        {
            if(((leftData[i] >> 4) & 0x0F ) > ((rightData[i] >> 4) & 0x0F)
                || (leftData[i] & 0x0F) > (rightData[i] & 0x0F))
                return true;
        }

        return hasLeftDecimalGreaterLength 
                ? CompareDecimals(leftData, rightData.size()) > 0
                : false;
    }

    bool operator<(const Decimal& left, const Decimal& right)
    {
        return !(left >= right);
    }

    bool operator<=(const Decimal& left, const Decimal& right)
    {
        return !(left > right);
    }

    int CompareDecimals(const vector<Constants::byte>& largerData, const int& startingIndex)
    {
        for(int i = startingIndex; i < largerData.size(); i++)
            if(((largerData[i] >> 4) & 0x0F) != 0 || (largerData[i] & 0x0F) != 0)
                return 1;

        return 0;
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
        const std::vector<Constants::byte> &left,
        const std::vector<Constants::byte> &right,
        const fraction_index_t& leftFractionIndex,
        const fraction_index_t& rightFractionIndex,
        const fraction_index_t &fractionIndex,
        const bool &isPositive){

        std::vector<Constants::byte> result;

        auto leftCopy = left;
        auto rightCopy = right;

        Decimal::PadFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);
        Decimal::PadNonFractionalParts(leftCopy, rightCopy, leftFractionIndex, rightFractionIndex);

        int carry = 0;

        for (int i = leftCopy.size() - 1; i > 0; i--) {
            // Extract digits from packed format
            const int leftHigh = (leftCopy[i] >> 4) & 0x0F;
            const int leftLow = leftCopy[i] & 0x0F;

            const int rightHigh = (rightCopy[i] >> 4) & 0x0F;
            const int rightLow = rightCopy[i] & 0x0F;

            // Add low digits
            int sumLow = leftLow + rightLow + carry;
            carry = sumLow / 10;
            sumLow %= 10;

            // Add high digits
            int sumHigh = leftHigh + rightHigh + carry;
            carry = sumHigh / 10;
            sumHigh %= 10;

            // Pack result back into byte
            Constants::byte packedByte = (sumHigh << 4) | sumLow;
            result.push_back(packedByte);
        }

        if (carry > 0)
            result.push_back(carry);

        ranges::reverse(result);
        result.insert(result.begin(), Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        return Decimal(result);
    }

    void Decimal::PadFractionalParts(
        std::vector<Constants::byte> &left,
        std::vector<Constants::byte> &right,
        const fraction_index_t &leftFractionIndex,
        const fraction_index_t &rightFractionIndex
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
        std::vector<Constants::byte> &left,
        std::vector<Constants::byte> &right,
        const fraction_index_t &leftFractionIndex,
        const fraction_index_t &rightFractionIndex){

        const int leftNonFracSize = leftFractionIndex / 2;
        const int rightNonFracSize = rightFractionIndex / 2;
        const int maxNonFracSize = max(leftNonFracSize, rightNonFracSize);

        // Pad left with leading zeros if needed
        if (leftNonFracSize < maxNonFracSize) {
            left.insert(left.begin() + 1, maxNonFracSize - leftNonFracSize, 0x00);
        }

        // Pad right with leading zeros if needed
        if (rightNonFracSize < maxNonFracSize) {
            right.insert(right.begin() + 1, maxNonFracSize - rightNonFracSize, 0x00);
        }
    }

    Decimal Decimal::Subtract(
        const std::vector<Constants::byte> &left,
        const std::vector<Constants::byte> &right,
        const fraction_index_t &fractionIndex,
        const bool &isPositive){

        vector<Constants::byte> result;
        int carry = 0;

        for (int i = static_cast<int>(left.size()) - 1; i >= 0; --i)
        {
            const int leftHigh = (left[i] >> 4) & 0x0F;
            const int leftLow = left[i] & 0x0F;

            const int rightHigh = (right[i] >> 4) & 0x0F;
            const int rightLow = right[i] & 0x0F;

            // Process low digits
            const int sumLow = leftLow - rightLow + carry;
            carry = sumLow / 10;

            // Process high digits
            const int sumHigh = leftHigh - rightHigh + carry;
            carry = sumHigh / 10;

            auto packedByte = static_cast<Constants::byte>(((sumHigh % 10) << 4) | (sumLow % 10));
            result.push_back(packedByte);
        }

        if (carry > 0)
            result.push_back(carry);

        ranges::reverse(result);

        result.insert(result.begin(), Decimal::CreateSignAndFractionByte(isPositive, fractionIndex));

        return Decimal(result);
    }

    bool Decimal::IsGreaterMagnitude(const std::vector<Constants::byte> &left, const std::vector<Constants::byte> &right){
        for (size_t i = 0; i < left.size(); i++) {
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

}


