#include "ByteMap.h"

#include <cstring>
#include <fstream>

namespace ByteMaps {
    ByteMap::ByteMap() = default;

    ByteMap::ByteMap(const Constants::byte_map_size_t &size)
    {
        this->data.resize(size, 0);
    }

    ByteMap::~ByteMap() = default;

    void ByteMap::SetPageIsAllocated(const Constants::byte_map_pos_t &pos, const bool& isAllocated)
    {
        this->CheckIndex(pos);
        if (isAllocated)
        {
            data[pos] |= ALLOCATION_MASK; // Set bit 0
            return;
        }

        data[pos] &= ~ALLOCATION_MASK; // Clear bit 0
    }

    bool ByteMap::IsAllocated(const Constants::byte_map_pos_t& pos) const
    {
        this->CheckIndex(pos);

        return (data[pos] & ALLOCATION_MASK) != 0;
    }

    void ByteMap::CheckIndex(const Constants::byte_map_pos_t& pos) const
    {
        if (pos >= data.size())
            throw std::out_of_range("Page index out of range.");
    }

    // Set the page type (bits 1-2)
    void ByteMap::SetPageType(const Constants::byte_map_pos_t& pos, const Constants::byte& type)
    {
      this->CheckIndex(pos);

      if (type > 0x07)  // Changed from 0x03 to 0x07 since we now have 3 bits
         throw std::invalid_argument("Page type must be between 0 and 7 (3 bits).");

      Constants::byte typeValue = (type << TYPE_SHIFT) & TYPE_MASK;
      data[pos] = (data[pos] & ~TYPE_MASK) | typeValue;

    }

    // Get the page type (bits 1-2)
    Constants::byte ByteMap::GetPageType(const Constants::byte_map_pos_t& pos) const
    {
      this->CheckIndex(pos);
      return (data[pos] & TYPE_MASK) >> TYPE_SHIFT; // Extract bits 1-3
    }

    // Set the free space percentage (bits 3-7)
    void ByteMap::SetFreeSpace(const Constants::byte_map_pos_t& pos, const Constants::byte& percentage)
    {
      this->CheckIndex(pos);

      if (percentage > 15) // Still 5 bits, but now using bits 4-8
          throw std::invalid_argument("Free space percentage must be between 0 and 31.");

      data[pos] = (data[pos] & ~SIZE_MASK) | (percentage & SIZE_MASK);
    }

    // Get the free space percentage (bits 3-7)
    Constants::page_size_t ByteMap::GetFreeSpace(const Constants::byte_map_pos_t& pos) const
    {
      this->CheckIndex(pos);
      return static_cast<Constants::page_size_t>(data[pos] & SIZE_MASK); // Extract bits 4-8
    }

    void ByteMap::SetByte(const Constants::byte_map_pos_t &position, const Constants::byte &value)
    {
        if (position < this->data.size())
            data[position] = value;
    }

    void ByteMap::GetDataFromFile(const vector<char> &data, Constants::page_offset_t &offset, const Constants::page_size_t& byteMapSize)
    {
        for (Constants::bit_map_size_t i = 0; i < byteMapSize; i++)
        {
            Constants::byte value;
            memcpy(&value, data.data() + offset, sizeof(Constants::byte));
            this->SetByte(i, value);

            offset += sizeof(Constants::byte);
        }
    }

    void ByteMap::WriteDataToFile(fstream *filePtr)
    {
        filePtr->write(reinterpret_cast<const char*>(this->data.data()), this->data.size() * sizeof(Constants::byte));
    }

    void ByteMap::Print() const
    {
        for (Constants::byte_map_pos_t i = 0; i < data.size(); i++)
            printf("Page %d: 0x%02X\n", i,  data[i]);
    }
}