#include "../../include/DataStructures/ByteMap.h"

#include <cstring>
#include <fstream>

#include "DataTypes/DataTypes.h"

namespace ByteMaps
{
    void ByteMap::CheckIndex(const byte_map_pos_t pos) const{
        if (pos >= data.size())
            throw std::out_of_range("Page index out of range.");
    }

    ByteMap::ByteMap(const byte_map_size_t size){
        this->data.resize(size, 0);
    }

    ByteMap::ByteMap() = default;

    ByteMap::~ByteMap() = default;

    void ByteMap::SetPageIsAllocated(const byte_map_pos_t pos, const bool isAllocated){
        this->CheckIndex(pos);
        if (isAllocated)
        {
            data[pos] |= ALLOCATION_MASK; // Set bit 0
            return;
        }

        data[pos] &= ~ALLOCATION_MASK; // Clear bit 0
    }

    bool ByteMap::IsAllocated(const byte_map_pos_t pos) const{
        this->CheckIndex(pos);
        return (data[pos] & ALLOCATION_MASK) != 0;
    }

    // Set the page type (bits 1-2)
    void ByteMap::SetPageType(const byte_map_pos_t pos, const byte_t type){
        this->CheckIndex(pos);

        if (type > 0x0F)  // 4 bits → max 15
            throw std::invalid_argument("Page type must be between 0 and 15 (4 bits).");

        const auto typeValue = (type << TYPE_SHIFT) & TYPE_MASK;
        data[pos] = (data[pos] & ~TYPE_MASK) | typeValue;
    }

    // Get the page type (bits 1-2)
    byte_t ByteMap::GetPageType(const byte_map_pos_t pos) const{
        this->CheckIndex(pos);
        return (data[pos] & TYPE_MASK) >> TYPE_SHIFT; // Extract bits 1-3
    }

    // Set the free space percentage (bits 5-7)
    void ByteMap::SetFreeSpace(const byte_map_pos_t pos, const byte_t percentage){
        this->CheckIndex(pos);

        if (percentage > 7) // 3 bits
            throw std::invalid_argument("Free space percentage must be between 0 and 7.");

        data[pos] = (data[pos] & ~SIZE_MASK) | (percentage & SIZE_MASK);
    }

    // Get the free space percentage (bits 3-7)
    page_size_t ByteMap::GetFreeSpace(const byte_map_pos_t pos) const
    {
        this->CheckIndex(pos);
        return static_cast<page_size_t>(data[pos] & SIZE_MASK); // Extract bits 4-8
    }

    void ByteMap::GetDataFromFile(const std::vector<char> &otherData, page_offset_t &offset, const page_size_t byteMapSize)
    {
        for (bit_map_size_t i = 0; i < byteMapSize; i++)
        {
            byte_t value;
            memcpy(&value, otherData.data() + offset, sizeof(byte_t));
            this->SetByte(i, value);

            offset += sizeof(byte_t);
        }
    }

    void ByteMap::WriteDataToFile(std::fstream *filePtr) const
    {
        filePtr->write(reinterpret_cast<const char*>(this->data.data()), this->data.size() * sizeof(byte_t));
    }

    void ByteMap::Print() const
    {
        for (byte_map_pos_t i = 0; i < data.size(); i++)
            printf("Page %d: 0x%02X\n", i,  data[i]);
    }

    void ByteMap::SetByte(const byte_map_pos_t position, const byte_t value)
    {
        if (position < this->data.size())
            data[position] = value;
    }
}
