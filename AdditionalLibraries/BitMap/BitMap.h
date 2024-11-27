#pragma once

#include <fstream>
#include <vector>
#include <iostream>
#include "../../Database/Constants.h"

using namespace std;

class BitMap {
    vector<byte> data;
    bit_map_size_t size;

    protected:
        void Resize(const bit_map_size_t& newSize);
        void SetByte(const bit_map_pos_t& position, const byte& value);

    public:
        BitMap();
        BitMap(const BitMap& bitMap);
        explicit BitMap(const bit_map_size_t& size);
        ~BitMap();
        void Set(const bit_map_pos_t& position, const bool& value);
        bool Get(const bit_map_pos_t& position) const;
        const bit_map_size_t& GetSize() const;
        bit_map_size_t GetSizeInBytes() const;
        void GetDataFromFile(const vector<char>& data, page_offset_t& offset);
        void WriteDataToFile(fstream* filePtr);
        void Print() const;
};