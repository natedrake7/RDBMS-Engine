#pragma once
#include <vector>
#include "../../../Database/Constants.h"

using namespace std;

namespace ByteMaps {

    static constexpr Constants::byte ALLOCATION_MASK = 0x80;  // bit 7
    static constexpr Constants::byte TYPE_MASK      = 0x78;  // bits 3–6 (0111 1000)
    static constexpr Constants::byte SIZE_MASK      = 0x07;  // bits 0–2 (0000 0111)

    static constexpr int TYPE_SHIFT = 3;  // shift left 3 to reach bits 3–6

    class ByteMap {
        vector<Constants::byte> data;

    protected:
        void CheckIndex(const Constants::byte_map_pos_t& pos) const;

    public:
        explicit ByteMap(const Constants::byte_map_size_t& size);
        ByteMap();
        ~ByteMap();

        void SetPageIsAllocated(const Constants::byte_map_pos_t& pos, const bool& isAllocated);
        [[nodiscard]] bool IsAllocated(const Constants::byte_map_pos_t& pos) const;

        void SetPageType(const Constants::byte_map_pos_t& pos, const Constants::byte& type);
        [[nodiscard]] Constants::byte GetPageType(const Constants::byte_map_pos_t& pos) const;
    
        void SetFreeSpace(const Constants::byte_map_pos_t& pos, const Constants::byte& percentage);
        [[nodiscard]] Constants::page_size_t GetFreeSpace(const Constants::byte_map_pos_t& pos) const;

        void GetDataFromFile(const vector<char> &data, Constants::page_offset_t &offset, const Constants::page_size_t& byteMapSize);
        void WriteDataToFile(fstream* filePtr);
        void Print() const;

        void SetByte(const Constants::byte_map_pos_t& position, const Constants::byte& value);
    
    };
}