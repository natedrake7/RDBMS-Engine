#include "Decimal.h"

#include <algorithm>
#include <cstdint>

namespace DataTypes {
    Decimal::Decimal() = default;

    Decimal::Decimal(const string& value)
    {
        string copiedValue(value);

        const bool isPositive = copiedValue[0] != '-';

        if (!isPositive || copiedValue[0] == '+')
            copiedValue.erase(0, 1);

        const fraction_index_t fractionIndex = Decimal::GetFractionIndex(copiedValue);

        const Constants::byte bitAndfractionIndexByte = (isPositive << 7) | (fractionIndex & 0x7F);

        this->bytes.push_back(bitAndfractionIndexByte);

        if (fractionIndex != 0)
            copiedValue.erase(fractionIndex , 1);

        for (int i = 0; i < copiedValue.size(); i+=2)
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

    Decimal operator+(const Decimal &left, const Decimal &right)
    {
        vector<Constants::byte> result;

        const auto& leftData = left.GetData();
        const auto& rightData = right.GetData();

        const bool isLeftPositive = left.IsPositive();
        const bool isRightPositive = right.IsPositive();

        const fraction_index_t leftFractionIndex = left.GetFractionIndex();
        const fraction_index_t rightFractionIndex = right.GetFractionIndex();

        auto leftIterator = leftData.rbegin() + 1;
        auto rightIterator = rightData.rbegin() + 1;
        int carry = 0;

        while (leftIterator != leftData.rend() || rightIterator != rightData.rend() || carry != 0)
        {

            const int left_digit = (leftIterator != leftData.rend())
                                ? *leftIterator++
                                : 0;
            
            const int right_digit = (rightIterator != rightData.rend())
                                ? *rightIterator++
                                : 0;

            const int sum = left_digit + right_digit + carry;
            
            result.push_back(static_cast<Constants::byte>(sum % 10)); // Store last digit.
            carry = sum / 100;                                         // Carry the overflow.
        }

        ranges::reverse(result);

        return Decimal(result);
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
}


