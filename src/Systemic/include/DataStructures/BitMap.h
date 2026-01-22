#pragma once
#include <vector>
#include "../../../DatabaseEngine/include/DatabaseConstants.h"

namespace ByteMaps{
    class BitMap{
        std::vector<byte_t> data;
        bit_map_size_t size;

    protected:
        void Resize(const bit_map_size_t &newSize);

    public:
        BitMap();
        BitMap(const BitMap &bitMap);
        explicit BitMap(const BitMap *bitMap);
        explicit BitMap(bit_map_size_t size, byte_t defaultValue = 0);

        BitMap(BitMap &&bitMap) noexcept;
        BitMap& operator=(BitMap &&bitMap) noexcept;
        ~BitMap();

        void Set(bit_map_pos_t position, bool value);
        void SetByte(bit_map_pos_t position, byte_t value);

        [[nodiscard]] bool Get(bit_map_pos_t position) const;
        [[nodiscard]] bit_map_size_t GetSize() const;
        [[nodiscard]] bit_map_size_t GetSizeInBytes() const;

        void GetDataFromFile(const object_t*& buffer, page_offset_t &offset);
        void GetDataFromFile(const std::vector<char> &buffer, page_offset_t &offset);
        void WriteDataToFile(std::fstream *filePtr);
        void WriteDataToFile(std::vector<char>* buffer, page_offset_t& pos)const;
        void WriteDataToBuffer(char*& buffer)const;
        void WriteDataToBuffer(object_t*& buffer, page_offset_t& offSet)const;
        void WriteDataToBuffer(std::vector<char> &buffer, page_offset_t& offSet)const;
        void Print() const;

        [[nodiscard]] bool Empty() const;

        [[nodiscard]] const std::vector<byte_t> &GetData() const;
        [[nodiscard]] std::vector<byte_t>& GetDataUnsafe();

        [[nodiscard]] bit_map_size_t GetSizeUnsafe() const;

        BitMap &operator=(const BitMap &bitMap);
    };
}
