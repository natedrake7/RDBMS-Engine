#pragma once
#include <fstream>
#include <vector>
#include "../../Database/Constants.h"

using namespace std;
using namespace Constants;

namespace ByteMaps
{
    class BitMap
    {
        vector<Constants::byte> data;
        bit_map_size_t size;
        bit_map_pos_t lastTrueIndex;

    protected:
        void Resize(const bit_map_size_t &newSize);
        void SetByte(const bit_map_pos_t &position, const Constants::byte &value);

    public:
        BitMap();
        BitMap(const BitMap &bitMap);
        explicit BitMap(const bit_map_size_t &size, const Constants::byte &defaultValue = 0);
        ~BitMap();

        void Set(const bit_map_pos_t &position, const bool &value);

        [[nodiscard]] bool Get(const bit_map_pos_t &position) const;
        [[nodiscard]] const bit_map_size_t &GetSize() const;
        [[nodiscard]] bit_map_size_t GetSizeInBytes() const;

        void GetDataFromFile(const vector<char> &data, page_offset_t &offset);
        void GetDataFromProtocol(const char*& data);
        void WriteDataToFile(fstream *filePtr);
        void WriteDataToProtocol(char*& data)const;
        void Print() const;

        [[nodiscard]] const vector<Constants::byte> &GetData() const;
        [[nodiscard]] vector<Constants::byte>& GetDataUnsafe();

        BitMap &operator=(const BitMap &bitMap);

        const bool HasAtLeastOneEntry();
    };
}
