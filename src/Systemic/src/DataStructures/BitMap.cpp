#include "../../include/DataStructures/BitMap.h"
#include <cstring>
#include <iostream>

#include <fstream>

#include "Memory/IAllocator.h"

namespace ByteMaps{
    BitMap::BitMap(object_t* data, const bit_map_size_t size)
        : _data(data), _size(size){}

    Int BitMap::HeapSize() const{
        return (this->_size + 7) / 8;
    }

    BitMap::BitMap(){
        this->_data = nullptr;
        this->_size = 0;
    }

    BitMap::BitMap(const BitMap &bitMap) = default;

    BitMap::BitMap(const Memory::IAllocator* allocator, const bit_map_size_t size, const bool defaultValue){
        this->_size = size;
        this->_data = static_cast<object_t*>(allocator->AllocateRaw(HeapSize(size)));
        std::memset(this->_data, defaultValue, HeapSize(size));
    }

    BitMap &BitMap::operator=(const BitMap &other){
        if (&other == this)
            return *this;

        this->_size = other._size;
        this->_data = other._data;

        return *this;
    }

    BitMap::BitMap(BitMap&& other) noexcept{
        this->_data = other._data;
        this->_size = other._size;

        other._data = nullptr;
    }

    BitMap& BitMap::operator=(BitMap&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->_size = other._size;

        other._data = nullptr;

        return *this;
    }

    BitMap::~BitMap() = default;

    void BitMap::FromExistingData(object_t* data, const bit_map_size_t size){
        this->_data = data;
        this->_size = size;
    }

    void BitMap::Set(const bit_map_pos_t position, const bool value) const{
        if (value){
            this->_data[position / 8] |= (1 << (position % 8)); // Set the bit
            return;
        }

        this->_data[position / 8] &= ~(1 << (position % 8)); // Clear the bit
    }

    bool BitMap::Get(const bit_map_pos_t position) const { return this->_data[position / 8] & (1 << (position % 8)); }

    bit_map_size_t BitMap::GetSize() const { return this->_size; }

    bit_map_size_t BitMap::GetSizeInBytes() const{
        return this->HeapSize() + sizeof(bit_map_size_t);
    }

    void BitMap::ReadFromPage(object_t* buffer, page_offset_t& offSet){
        std::memcpy(&this->_size, buffer + offSet, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        this->_data = buffer + offSet;
        offSet += this->HeapSize();
    }

    void BitMap::WriteDataToBuffer(char *&buffer) const{
        std::memcpy(buffer, &this->_size, sizeof(bit_map_size_t));
        buffer += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer, this->_data, heapSize);
        buffer += heapSize;
    }

    void BitMap::WriteDataToBuffer(object_t*& buffer, page_offset_t& offSet) const{
        std::memcpy(buffer + offSet, &this->_size, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer + offSet, this->_data, heapSize);
        offSet += heapSize;
    }

    void BitMap::Print() const{
        for (bit_map_pos_t i = 0; i < this->_size; i++)
            std::cout << this->Get(i);

        std::cout << std::endl;
    }

    bool BitMap::Empty() const{
        return this->_size == 0;
    }

    const byte_t* BitMap::DataPtr() const{
        return this->_data;
    }

    byte_t* BitMap::DataPtrUnsafe() const{
        return this->_data;
    }

    bit_map_size_t BitMap::GetSizeUnsafe() const { return this->_size; }

    Int BitMap::HeapSize(const Int size){
        return (size + 7) / 8;
    }
}
