#pragma once
#include <vector>
#include "../../../DatabaseEngine/include/Constants.h"

namespace ByteMaps
{
    class BitMap
    {
        std::vector<byte_t> data;
        bit_map_size_t size;
        bit_map_pos_t lastTrueIndex;

    protected:
        void Resize(const bit_map_size_t &newSize);

    public:
        BitMap();
        BitMap(const BitMap &bitMap);
        explicit BitMap(const BitMap *bitMap);
        explicit BitMap(const bit_map_size_t &size, const byte_t &defaultValue = 0);
        ~BitMap();

        void Set(const bit_map_pos_t &position, const bool &value);
        void SetByte(const bit_map_pos_t &position, const byte_t &value);

        [[nodiscard]] bool Get(const bit_map_pos_t &position) const;
        [[nodiscard]] const bit_map_size_t &GetSize() const;
        [[nodiscard]] bit_map_size_t GetSizeInBytes() const;

        void GetDataFromFile(const vector<char> &buffer, page_offset_t &offset);
        void GetDataFromFile(const vector<char> &buffer, uint32_t &offset);
        void WriteDataToFile(fstream *filePtr);
        void WriteDataToFile(std::vector<char>* buffer, uint32_t& pos)const;
        void WriteDataToProtocol(char*& data)const;
        void Print() const;

        [[nodiscard]] const vector<byte_t> &GetData() const;
        [[nodiscard]] vector<byte_t>& GetDataUnsafe();

        [[nodiscard]] bit_map_size_t& GetSizeUnsafe();

        BitMap &operator=(const BitMap &bitMap);

         bool HasAtLeastOneEntry();
    };
}
