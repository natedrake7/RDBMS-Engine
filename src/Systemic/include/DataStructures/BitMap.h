#pragma once
#include <vector>
#include "../DataTypes/DataTypes.h"

namespace Memory
{
    class IAllocator;
}

namespace ByteMaps{
    class BitMap final{
        object_t* _data;
        bit_map_size_t size;

        bool isReferencingData;

    protected:
        void Resize(bit_map_size_t newSize);
        Int HeapSize()const;

        BitMap(object_t* data, bit_map_size_t size);

    public:
        BitMap();
        BitMap(const BitMap &bitMap);
        explicit BitMap(const BitMap *bitMap);
        explicit BitMap(const ::Memory::IAllocator* allocator, bit_map_size_t size, byte_t defaultValue = 0);

        static BitMap FromExistingData(object_t *data, bit_map_size_t size);

        BitMap &operator=(const BitMap &other);

        BitMap(BitMap &&other) noexcept;
        BitMap& operator=(BitMap &&other) noexcept;
        ~BitMap();

        void Set(bit_map_pos_t position, bool value);
        [[nodiscard]] bool Get(bit_map_pos_t position) const;
        [[nodiscard]] bit_map_size_t GetSize() const;
        [[nodiscard]] bit_map_size_t GetSizeInBytes() const;

        void GetDataFromFile(const object_t* buffer, page_offset_t &offset);
        void GetDataFromFile(const object_t* buffer, page_offset_t &offset, Int otherSize);
        void GetDataFromFile(const std::vector<char> &buffer, page_offset_t &offset);
        void WriteDataToFile(std::fstream *filePtr);
        void WriteDataToFile(std::vector<char>* buffer, page_offset_t& pos)const;
        void WriteDataToBuffer(char*& buffer)const;
        void WriteDataToBuffer(object_t*& buffer, page_offset_t& offSet)const;
        void WriteDataToBuffer(std::vector<char> &buffer, page_offset_t& offSet)const;
        void Print() const;

        [[nodiscard]] bool Empty() const;

        // [[nodiscard]] const std::vector<byte_t> &GetData() const;
        // [[nodiscard]] std::vector<byte_t>& GetDataUnsafe();

        [[nodiscard]] const byte_t* DataPtr() const;
        [[nodiscard]] byte_t* DataPtrUnsafe() const;

        [[nodiscard]] bit_map_size_t GetSizeUnsafe() const;

        static Int HeapSize(Int size);

    };
}
