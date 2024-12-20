#include "BitMap.h"
#include <iostream>

namespace ByteMaps
{
    BitMap::BitMap()
    {
        this->size = 0;
        this->lastTrueIndex = 0;
    }

    BitMap::BitMap(const BitMap &bitMap)
    {
        this->size = bitMap.size;
        this->data = bitMap.data;
        this->lastTrueIndex = bitMap.lastTrueIndex;
    }

    BitMap::BitMap(const Constants::bit_map_size_t &size, const Constants::byte &defaultValue) : size(size)
    {
        this->data.resize((size + 7) / 8, defaultValue);
    }

    BitMap::~BitMap() = default;

    void BitMap::Set(const Constants::bit_map_pos_t &position, const bool &value)
    {
        if (position >= this->size)
            this->Resize(position + 1);

        if (value)
        {
            data[position / 8] |= (1 << (position % 8)); // Set the bit
            return;
        }

        data[position / 8] &= ~(1 << (position % 8)); // Clear the bit
    }

    bool BitMap::Get(const Constants::bit_map_pos_t &position) const { return data[position / 8] & (1 << (position % 8)); }

    const Constants::bit_map_size_t &BitMap::GetSize() const { return this->size; }

    Constants::bit_map_size_t BitMap::GetSizeInBytes() const { return this->data.size() + sizeof(Constants::block_size_t); }

    void BitMap::SetByte(const Constants::bit_map_pos_t &position, const Constants::byte &value)
    {
        if (position < this->data.size())
            data[position] = value;
    }

    void BitMap::GetDataFromFile(const vector<char> &data, Constants::page_offset_t &offset)
    {
        memcpy(&this->size, data.data() + offset, sizeof(Constants::bit_map_size_t));
        offset += sizeof(Constants::bit_map_size_t);

        const Constants::bit_map_size_t &bytesToRead = (this->size + 7) / 8;

        if (this->data.empty())
            this->data.resize(bytesToRead);

        for (Constants::bit_map_size_t i = 0; i < bytesToRead; i++)
        {
            Constants::byte value;
            memcpy(&value, data.data() + offset, sizeof(Constants::byte));
            this->SetByte(i, value);

            offset += sizeof(Constants::byte);
        }
    }

    void BitMap::WriteDataToFile(fstream *filePtr)
    {
        filePtr->write(reinterpret_cast<char *>(&this->size), sizeof(Constants::bit_map_size_t));
        filePtr->write(reinterpret_cast<char *>(this->data.data()), this->data.size() * sizeof(Constants::byte));
    }

    void BitMap::Print() const
    {
        for (Constants::bit_map_pos_t i = 0; i < size; i++)
            cout << this->Get(i);

        cout << endl;
    }

    const vector<Constants::byte> &BitMap::GetData() const { return this->data; }

    BitMap &BitMap::operator=(const BitMap &bitMap)
    {
        if (&bitMap == this)
            return *this;

        this->data = bitMap.GetData();

        return *this;
    }

    const bool BitMap::HasAtLeastOneEntry()
    {
        if (this->lastTrueIndex < this->size)
        {
            const bool indexValue = this->Get(this->lastTrueIndex);

            if (indexValue)
                return indexValue;
        }

        for (bit_map_size_t i = 0; i < this->size; i++)
        {
            const bool hasValue = this->Get(i);

            if (hasValue)
            {
                this->lastTrueIndex = i;
                return true;
            }
        }

        return false;
    }

    void BitMap::Resize(const Constants::bit_map_size_t &newSize)
    {
        uint16_t newByteCount = (newSize + 7) / 8;
        this->data.resize(newByteCount, 0);
        this->size = newSize;
    }
}
