#pragma once
#include <vector>
#include "../../../DatabaseEngine/include/Constants.h"

using namespace std;

namespace ByteMaps {

    static constexpr byte_t ALLOCATION_MASK = 0x80;  // bit 7
    static constexpr byte_t TYPE_MASK      = 0x78;  // bits 3–6 (0111 1000)
    static constexpr byte_t SIZE_MASK      = 0x07;  // bits 0–2 (0000 0111)

    static constexpr int TYPE_SHIFT = 3;  // shift left 3 to reach bits 3–6

    class ByteMap {
        vector<byte_t> data;

    protected:
        void CheckIndex(const byte_map_pos_t& pos) const;

    public:
        explicit ByteMap(const byte_map_size_t& size);
        ByteMap();
        ~ByteMap();

        void SetPageIsAllocated(const byte_map_pos_t& pos, const bool& isAllocated);
        [[nodiscard]] bool IsAllocated(const byte_map_pos_t& pos) const;

        void SetPageType(const byte_map_pos_t& pos, const byte_t& type);
        [[nodiscard]] byte_t GetPageType(const byte_map_pos_t& pos) const;
    
        void SetFreeSpace(const byte_map_pos_t& pos, const byte_t& percentage);
        [[nodiscard]] page_size_t GetFreeSpace(const byte_map_pos_t& pos) const;

        void GetDataFromFile(const vector<char> &otherData, page_offset_t &offset, const page_size_t& byteMapSize);
        void WriteDataToFile(fstream* filePtr);
        void Print() const;

        void SetByte(const byte_map_pos_t& position, const byte_t& value);
    
    };
}