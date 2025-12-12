#include "../../include/DataStructures/BitMap.h"
#include <cstring>
#include <iostream>

#include "../../../Database/include/Constants.h"

#include <fstream>

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

    BitMap::BitMap(const BitMap *bitMap){
        this->size = bitMap->size;
        this->data = bitMap->data;
        this->lastTrueIndex = bitMap->lastTrueIndex;
    }

    BitMap::BitMap(const bit_map_size_t &size, const byte_t &defaultValue) : size(size)
    {
        this->data.resize((size + 7) / 8, defaultValue);
    }

    BitMap::~BitMap() = default;

    void BitMap::Set(const bit_map_pos_t &position, const bool &value)
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

    bool BitMap::Get(const bit_map_pos_t &position) const { return data[position / 8] & (1 << (position % 8)); }

    const bit_map_size_t &BitMap::GetSize() const { return this->size; }

    bit_map_size_t BitMap::GetSizeInBytes() const { return this->data.size() + sizeof(bit_map_size_t); }

    void BitMap::SetByte(const bit_map_pos_t &position, const byte_t &value)
    {
        if (position < this->data.size())
            data[position] = value;
    }

    void BitMap::GetDataFromFile(const vector<char> &buffer, page_offset_t &offset)
    {
        memcpy(&this->size, buffer.data() + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t &bytesToRead = (this->size + 7) / 8;

        if (this->data.empty())
            this->data.resize(bytesToRead);

        for (bit_map_size_t i = 0; i < bytesToRead; i++)
        {
            byte_t value;
            memcpy(&value, buffer.data() + offset, sizeof(byte_t));
            this->SetByte(i, value);

            offset += sizeof(byte_t);
        }
    }

    void BitMap::GetDataFromFile(const vector<char> &buffer, uint32_t &offset){
        memcpy(&this->size, buffer.data() + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t &bytesToRead = (this->size + 7) / 8;

        if (this->data.empty())
            this->data.resize(bytesToRead);

        for (bit_map_size_t i = 0; i < bytesToRead; i++)
        {
            byte_t value;
            memcpy(&value, buffer.data() + offset, sizeof(byte_t));
            this->SetByte(i, value);

            offset += sizeof(byte_t);
        }
    }

    void BitMap::WriteDataToFile(fstream *filePtr)
    {
        filePtr->write(reinterpret_cast<char *>(&this->size), sizeof(bit_map_size_t));
        filePtr->write(reinterpret_cast<char *>(this->data.data()), this->data.size() * sizeof(byte_t));
    }

    void BitMap::WriteDataToFile(std::vector<char>* buffer, uint32_t& pos)const
    {
        memcpy(buffer->data() + pos, &this->size, sizeof(bit_map_size_t));
        pos += sizeof(bit_map_size_t);

        memcpy(buffer->data() + pos, this->data.data(), this->data.size() * sizeof(byte_t));
        pos += this->data.size() * sizeof(byte_t);
    }

    void BitMap::WriteDataToProtocol(char *&data) const{
        memcpy(data, &this->size, sizeof(bit_map_size_t));
        data += sizeof(bit_map_size_t);

        const int dataSize = this->data.size() * sizeof(byte_t);
        
        memcpy(data, this->data.data(), dataSize);
        data += dataSize;
    }

    void BitMap::Print() const
    {
        for (bit_map_pos_t i = 0; i < size; i++)
            cout << this->Get(i);

        cout << endl;
    }

    const vector<byte_t> &BitMap::GetData() const { return this->data; }

    vector<byte_t> & BitMap::GetDataUnsafe(){ return this->data; }

    bit_map_size_t & BitMap::GetSizeUnsafe(){ return this->size; }

    BitMap &BitMap::operator=(const BitMap &bitMap)
    {
        if (&bitMap == this)
            return *this;

        this->data = bitMap.GetData();
        this->size = bitMap.GetSize();

        return *this;
    }

     bool BitMap::HasAtLeastOneEntry()
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

    void BitMap::Resize(const bit_map_size_t &newSize)
    {
        const uint16_t newByteCount = (newSize + 7) / 8;
        this->data.resize(newByteCount, 0);
        this->size = newSize;
    }
}
