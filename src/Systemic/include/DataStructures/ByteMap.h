#pragma once
#include <vector>
#include <fstream>
#include "../DataTypes/DataTypes.h"

namespace ByteMaps {

    static constexpr byte_t ALLOCATION_MASK = 0x80;  // bit 7
    static constexpr byte_t TYPE_MASK      = 0x78;  // bits 3–6 (0111 1000)
    static constexpr byte_t SIZE_MASK      = 0x07;  // bits 0–2 (0000 0111)

    static constexpr int TYPE_SHIFT = 3;  // shift left 3 to reach bits 3–6

    class ByteMap {
        std::vector<byte_t> data;

    protected:
        void CheckIndex(byte_map_pos_t pos) const;

    public:
        explicit ByteMap(byte_map_size_t size);
        ByteMap();
        ~ByteMap();

        void SetPageIsAllocated(byte_map_pos_t pos, bool isAllocated);
        [[nodiscard]] bool IsAllocated(byte_map_pos_t pos) const;

        void SetPageType(byte_map_pos_t pos, byte_t type);
        [[nodiscard]] byte_t GetPageType(byte_map_pos_t pos) const;
    
        void SetFreeSpace(byte_map_pos_t pos, byte_t percentage);
        [[nodiscard]] page_size_t GetFreeSpace(byte_map_pos_t pos) const;

        void GetDataFromFile(const std::vector<char> &otherData, page_offset_t &offset, page_size_t byteMapSize);
        void WriteDataToFile(std::fstream* filePtr) const;
        void Print() const;

        void SetByte(byte_map_pos_t position, byte_t value);
    
    };
}
