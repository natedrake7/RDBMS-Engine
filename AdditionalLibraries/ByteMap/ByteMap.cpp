#include "ByteMap.h"

ByteMap::ByteMap() = default;

ByteMap::ByteMap(const byte_map_size_t &size)
{
    this->data.resize(size, 0);
}

ByteMap::~ByteMap() = default;

void ByteMap::SetPageIsAllocated(const byte_map_pos_t &pos, const bool& isAllocated)
{
    this->CheckIndex(pos);
    if (isAllocated)
    {
        data[pos] |= 0x01; // Set bit 0
        return;
    }

    data[pos] &= ~0x01; // Clear bit 0
}

bool ByteMap::IsAllocated(const byte_map_pos_t& pos) const
{
    this->CheckIndex(pos);
    return data[pos] & 0x01;
}

void ByteMap::CheckIndex(const byte_map_pos_t& pos) const
{
    if (pos >= data.size())
        throw std::out_of_range("Page index out of range.");
}

// Set the page type (bits 1-2)
void ByteMap::SetPageType(const byte_map_pos_t& pos, const byte& type)
{
    this->CheckIndex(pos);
    if (type > 0x03)
        throw std::invalid_argument("Page type must be between 0 and 3 (2 bits).");

    data[pos] = (data[pos] & ~0x06) | (type << 1); // Clear bits 1-2 and set new type
}

// Get the page type (bits 1-2)
byte ByteMap::GetPageType(const byte_map_pos_t& pos) const
{
    this->CheckIndex(pos);
    return (data[pos] & 0x06) >> 1; // Extract bits 1-2
}

// Set the free space percentage (bits 3-7)
void ByteMap::SetFreeSpace(const byte_map_pos_t& pos, const byte& percentage)
{
    this->CheckIndex(pos);
    
    if (percentage > 31) // 5 bits can represent values from 0 to 31
        throw std::invalid_argument("Free space percentage must be between 0 and 31.");

    data[pos] = (data[pos] & 0x07) | (percentage << 3); // Clear bits 3-7 and set new percentage
}

// Get the free space percentage (bits 3-7)
byte ByteMap::GetFreeSpace(const byte_map_pos_t& pos) const
{
    this->CheckIndex(pos);
    return (data[pos] & 0xF8) >> 3; // Extract bits 3-7
}

void ByteMap::SetByte(const byte_map_pos_t &position, const byte &value)
{
    if (position < this->data.size())
        data[position] = value;
}

void ByteMap::GetDataFromFile(const vector<char> &data, page_offset_t &offset, const page_size_t& byteMapSize)
{
    for (bit_map_size_t i = 0; i < byteMapSize; i++)
    {
        byte value;
        memcpy(&value, data.data() + offset, sizeof(byte));
        this->SetByte(i, value);

        offset += sizeof(byte);
    }
}

void ByteMap::WriteDataToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<const char*>(this->data.data()), this->data.size() * sizeof(byte));
}
