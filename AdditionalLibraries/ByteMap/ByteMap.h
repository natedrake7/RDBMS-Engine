#pragma once
#include <vector>
#include <stdexcept>
#include <fstream>

#include "../../Database/Constants.h"

using namespace std;

class ByteMap {
    vector<byte> data;

    protected:
        void CheckIndex(const byte_map_pos_t& pos) const;

    public:
        explicit ByteMap(const byte_map_size_t& size);
        ByteMap();
        ~ByteMap();

        void SetPageIsAllocated(const byte_map_pos_t& pos, const bool& isAllocated);
        bool IsAllocated(const byte_map_pos_t& pos) const;

        void SetPageType(const byte_map_pos_t& pos, const byte& type);
        byte GetPageType(const byte_map_pos_t& pos) const;
    
        void SetFreeSpace(const byte_map_pos_t& pos, const byte& percentage);
        byte GetFreeSpace(const byte_map_pos_t& pos) const;

        void GetDataFromFile(const vector<char> &data, page_offset_t &offset, const page_size_t& byteMapSize);
        void WriteDataToFile(fstream* filePtr);
        void Print() const;

        void SetByte(const byte_map_pos_t& position, const byte& value);
    
};
