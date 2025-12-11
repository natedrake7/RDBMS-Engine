#pragma once
#include <vector>
#include "../../../Database/include/Constants.h"

using namespace std;

namespace ByteMaps
{
    class BitMap
    {
        vector<Constants::byte> data;
        Constants::bit_map_size_t size;
        Constants::bit_map_pos_t lastTrueIndex;

    protected:
        void Resize(const Constants::bit_map_size_t &newSize);

    public:
        BitMap();
        BitMap(const BitMap &bitMap);
        explicit BitMap(const BitMap *bitMap);
        explicit BitMap(const Constants::bit_map_size_t &size, const Constants::byte &defaultValue = 0);
        ~BitMap();

        void Set(const Constants::bit_map_pos_t &position, const bool &value);
        void SetByte(const Constants::bit_map_pos_t &position, const Constants::byte &value);

        [[nodiscard]] bool Get(const Constants::bit_map_pos_t &position) const;
        [[nodiscard]] const Constants::bit_map_size_t &GetSize() const;
        [[nodiscard]] Constants::bit_map_size_t GetSizeInBytes() const;

        void GetDataFromFile(const vector<char> &buffer, Constants::page_offset_t &offset);
        void GetDataFromFile(const vector<char> &buffer, uint32_t &offset);
        void WriteDataToFile(fstream *filePtr);
        void WriteDataToFile(std::vector<char>* buffer, uint32_t& pos)const;
        void WriteDataToProtocol(char*& data)const;
        void Print() const;

        [[nodiscard]] const vector<Constants::byte> &GetData() const;
        [[nodiscard]] vector<Constants::byte>& GetDataUnsafe();

        [[nodiscard]] Constants::bit_map_size_t& GetSizeUnsafe();

        BitMap &operator=(const BitMap &bitMap);

         bool HasAtLeastOneEntry();
    };
}
