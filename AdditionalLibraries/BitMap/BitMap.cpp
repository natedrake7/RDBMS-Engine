#include "BitMap.h"

BitMap::BitMap()
{
    this->size = 0;
}

BitMap::BitMap(const bit_map_size_t& size) : size(size)
{
    this->data.resize((size + 7) / 8, 0);
}

BitMap::~BitMap() = default;

void BitMap::Set(const bit_map_pos_t& position, const bool& value)
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

bool BitMap::Get(const bit_map_pos_t& position) const { return data[position / 8] & (1 << (position % 8)); }

const bit_map_size_t& BitMap::GetSize() const { return this->size; }

void BitMap::SetByte(const bit_map_pos_t& position, const byte& value)
{
    if (position < this->data.size())
        data[position] = value;
}

void BitMap::GetDataFromFile(const vector<char>& data, page_offset_t& offset)
{
    memcpy(&this->size, data.data() + offset, sizeof(bit_map_size_t));
    offset += sizeof(bit_map_size_t);
    
    const bit_map_size_t& bytesToRead = (this->size + 7) / 8;
    for (bit_map_size_t i = 0; i < bytesToRead; i++)
    {
        byte value;
        memcpy(&value, data.data() + offset, sizeof(byte));
        offset += sizeof(byte);
        this->SetByte(i, value);
    }
}

void BitMap::WriteDataToFile(fstream* filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->size), sizeof(bit_map_size_t));
    filePtr->write(reinterpret_cast<char*>(this->data.data()), this->data.size());
}

void BitMap::Print() const
{
    for (bit_map_pos_t i = 0; i < size; i++)
        cout<< this->Get(i);

    cout<< endl;
}

void BitMap::Resize(const bit_map_size_t& newSize)
{
    uint16_t newByteCount = ( newSize + 7 ) / 8;
    this->data.resize(newByteCount, 0);
    this->size = newSize;
}


