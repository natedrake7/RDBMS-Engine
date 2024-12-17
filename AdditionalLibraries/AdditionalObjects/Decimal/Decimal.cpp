#include "Decimal.h"

Decimal::Decimal() = default;

Decimal::Decimal(const string& value)
{
    string copiedValue(value);

    const bool isPositive = copiedValue[0] != '-';

    if (!isPositive || copiedValue[0] == '+')
        copiedValue.erase(0, 1);

    const Constants::fraction_index_t fractionIndex = Decimal::GetFractionIndex(copiedValue);

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
    this->bytes = vector<Constants::byte>(data, data + dataSize);
}

Decimal::~Decimal() = default;

bool Decimal::IsPositive() const { return ( this->bytes.at(0) >> 7 ) & 0x01; }

Constants::fraction_index_t Decimal::GetFractionIndex() const { return static_cast<Constants::fraction_index_t>(this->bytes.at(0) & 0x7F); }

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

    const Constants::fraction_index_t fractionIndex = Decimal::GetFractionIndex() + ( isPositive ? 0 : 1);

    result.insert(fractionIndex,  ".");

    return result;
}

const Constants::byte* Decimal::GetRawData() const { return this->bytes.data(); }

int Decimal::GetRawDataSize() const { return this->bytes.size(); }

Constants::fraction_index_t Decimal::GetFractionIndex(const string& value)
{
    const int decimalPos = value.find('.');

    return (decimalPos != string::npos) 
                        ? decimalPos 
                        : 0;
}

