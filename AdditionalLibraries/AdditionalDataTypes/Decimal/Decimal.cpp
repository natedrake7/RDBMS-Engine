#include "Decimal.h"

#include "../../SafeConverter/SafeConverter.h"

#include <algorithm>
#include <iostream>

namespace DataTypes {
    Decimal::Decimal() = default;

    Decimal::Decimal(const string& value)
    {
        if (value.empty())
            return;

        string copiedValue(value);

        const bool isPositive = copiedValue[0] != '-';

        if (!isPositive || copiedValue[0] == '+')
            copiedValue.erase(0, 1);

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex(copiedValue);

        const Constants::byte signAndFractionPoint = (isPositive << 7) | (fractionIndex & 0x7F);

        this->bytes.push_back(signAndFractionPoint);

        //invalid string
        if (fractionIndex != 0)
            copiedValue.erase(fractionIndex , 1);

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
        this->bytes = vector(data, data + dataSize);
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

        auto leftData = left.GetData();
        auto rightData = right.GetData();

        const bool leftSign = left.IsPositive();
        const bool rightSign = right.IsPositive();

        const fraction_index_t leftFractionIndex = left.GetFractionIndex();
        const fraction_index_t rightFractionIndex = right.GetFractionIndex();

        //erase sign and fraction point (they will be recomputed)
        leftData.erase(leftData.begin());
        rightData.erase(rightData.begin());

        const auto leftSize = leftData.size() * 2;
        const auto rightSize = rightData.size() * 2;

        const auto leftFractionalPart = leftSize - leftFractionIndex;
        const auto rightFractionalPart = rightSize - rightFractionIndex;

        const auto leftIntegerPart = leftSize - leftFractionalPart;
        const auto rightIntegerPart = rightSize - rightFractionalPart;

        const auto fractionIndex = leftFractionIndex > rightFractionIndex ? leftFractionIndex : rightFractionIndex;

        // if (leftSign == rightSign)
        //     return Decimal::Add(leftData, rightData, fractionIndex, leftSign);

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
        const int decimalPos = value.find('.');

        return (decimalPos != string::npos) 
                            ? decimalPos 
                            : 0;
    }

    Decimal Decimal::Add(
        const std::vector<Constants::byte> &left,
        const std::vector<Constants::byte> &right,
        const fraction_index_t& leftFractionIndex,
        const fraction_index_t& rightFractionIndex,
        const fraction_index_t &fractionIndex,
        const bool &isPositive){
        std::vector<Constants::byte> result;

        int carry = Decimal::FractionalAdd(left, right, leftFractionIndex, rightFractionIndex, result);

        const auto leftSize = left.size() * 2 / leftFractionIndex; //+ ();
        const auto rightSize = right.size();

        const auto biggestSize = leftSize > rightSize;

        for (int i = static_cast<int>(left.size()) - 1; i >= 0; --i)
        {
            const int leftHigh = (left[i] >> 4) & 0x0F;
            const int leftLow = left[i] & 0x0F;

            const int rightHigh = (right[i] >> 4) & 0x0F;
            const int rightLow = right[i] & 0x0F;

            // Process low digits
            const int sumLow = leftLow + rightLow + carry;
            carry = sumLow / 10;

            // Process high digits
            const int sumHigh = leftHigh + rightHigh + carry;
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

     int Decimal::FractionalAdd(
        const std::vector<Constants::byte> &left,
        const std::vector<Constants::byte> &right,
        const fraction_index_t &leftFractionIndex,
        const fraction_index_t &rightFractionIndex,
        vector<Constants::byte>& result){

            const int leftSize = left.size() * 2 - leftFractionIndex;
            const int rightSize = right.size() * 2 - rightFractionIndex;

            const int biggestSize = leftSize > rightSize ? leftSize : rightSize;

            int carry = 0;
            for (int i = biggestSize - 1; i >= 0; i--) {
                const int leftHigh = (leftSize > i) ? (left[i] >> 4) & 0x0F : 0;
                const int leftLow = (leftSize > i) ? left[i] & 0x0F : 0;

                const int rightHigh = (rightSize > i) ? (right[i] >> 4) & 0x0F : 0;
                const int rightLow = (rightSize > i) ? right[i] & 0x0F : 0;

                // Process low digits
                const int sumLow = leftLow + rightLow + carry;
                carry = sumLow / 10;

                // Process high digits
                const int sumHigh = leftHigh + rightHigh + carry;
                carry = sumHigh / 10;

                auto packedByte = static_cast<Constants::byte>(((sumHigh % 10) << 4) | (sumLow % 10));
                result.push_back(packedByte);
            }

        return carry;
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


