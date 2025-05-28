#pragma once
#include <vector>
#include <stdexcept>
#include <fstream>

#include "../../Database/Constants.h"

using namespace std;
using namespace Constants;

namespace ByteMaps {

    static constexpr Constants::byte ALLOCATION_MASK = 0x80;  // 1000 0000
    static constexpr Constants::byte TYPE_MASK      = 0x70;  // 0111 0000
    static constexpr Constants::byte SIZE_MASK      = 0x0F;  // 0000 1111

    static constexpr int TYPE_SHIFT = 4;

    class ByteMap {
        vector<Constants::byte> data;

    protected:
        void CheckIndex(const byte_map_pos_t& pos) const;

    public:
        explicit ByteMap(const byte_map_size_t& size);
        ByteMap();
        ~ByteMap();

        void SetPageIsAllocated(const byte_map_pos_t& pos, const bool& isAllocated);
        bool IsAllocated(const byte_map_pos_t& pos) const;

        void SetPageType(const byte_map_pos_t& pos, const Constants::byte& type);
        Constants::byte GetPageType(const byte_map_pos_t& pos) const;
    
        void SetFreeSpace(const byte_map_pos_t& pos, const Constants::byte& percentage);
        page_size_t GetFreeSpace(const byte_map_pos_t& pos) const;

        void GetDataFromFile(const vector<char> &data, page_offset_t &offset, const page_size_t& byteMapSize);
        void WriteDataToFile(fstream* filePtr);
        void Print() const;

        void SetByte(const byte_map_pos_t& position, const Constants::byte& value);
    
    };
}