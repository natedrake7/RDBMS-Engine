#include "../../include/DataStructures/BitMap.h"
#include <cstring>
#include <iostream>

#include <fstream>

namespace ByteMaps{
    void BitMap::Resize(const bit_map_size_t newSize){
        if (this->isReferencingData)
            throw std::runtime_error("BitMap::Resize: Cannot resize a BitMap that is referencing external data.");

        const auto newByteCount = (newSize + 7) / 8;
        this->_data = static_cast<object_t*>(std::realloc(this->_data, newByteCount));

        for (int i = this->size; i < newSize; i++)
            this->Set(i, false);

        this->size = newSize;
    }

    Int BitMap::HeapSize() const{
        return (this->size + 7) / 8;
    }

    BitMap::BitMap(){
        this->_data = nullptr;
        this->size = 0;

        this->isReferencingData = false;
    }

    BitMap::BitMap(const BitMap &bitMap){
        this->size = bitMap.size;

        const auto heapSize = this->HeapSize();
        this->_data = static_cast<object_t*>(std::malloc(heapSize));
        std::memcpy(this->_data, bitMap._data, heapSize);

        this->isReferencingData = false;
    }

    BitMap::BitMap(const BitMap *bitMap){
        this->size = bitMap->size;

        const auto heapSize = this->HeapSize();
        this->_data = static_cast<object_t*>(std::malloc(heapSize));
        std::memcpy(this->_data, bitMap->_data, heapSize);

        this->isReferencingData = false;
    }

    BitMap::BitMap(const bit_map_size_t size, const byte_t defaultValue){
        this->size = size;

        const auto heapSize = this->HeapSize();
        this->_data = static_cast<object_t*>(std::malloc(heapSize));

        std::memset(this->_data, defaultValue, heapSize);
        this->isReferencingData = false;
    }

    BitMap BitMap::FromExistingData(object_t* data, const bit_map_size_t size){
        return BitMap(data, size);
    }

    BitMap::BitMap(object_t* data, const bit_map_size_t size){
        this->_data = data;
        this->size = size;
        this->isReferencingData = true;
    }

    BitMap &BitMap::operator=(const BitMap &other){
        if (&other == this)
            return *this;

        this->size = other.GetSize();

        const auto heapSize = this->HeapSize();
        this->_data = static_cast<object_t*>(std::malloc(heapSize));
        std::memcpy(this->_data, other._data, heapSize);

        return *this;
    }

    BitMap::BitMap(BitMap&& other) noexcept{
        this->_data = other._data;
        this->size = other.size;
        this->isReferencingData = other.isReferencingData;

        other._data = nullptr;
    }

    BitMap& BitMap::operator=(BitMap&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->size = other.size;
        this->isReferencingData = other.isReferencingData;

        other._data = nullptr;

        return *this;
    }

    BitMap::~BitMap(){
        if (!this->isReferencingData)
            std::free(this->_data);
    }

    void BitMap::Set(const bit_map_pos_t position, const bool value){
        if (position >= this->size)
            this->Resize(position + 1);

        if (value){
            this->_data[position / 8] |= (1 << (position % 8)); // Set the bit
            return;
        }

        this->_data[position / 8] &= ~(1 << (position % 8)); // Clear the bit
    }

    bool BitMap::Get(const bit_map_pos_t position) const { return this->_data[position / 8] & (1 << (position % 8)); }

    bit_map_size_t BitMap::GetSize() const { return this->size; }

    bit_map_size_t BitMap::GetSizeInBytes() const{
        return this->HeapSize() + sizeof(bit_map_size_t);
    }

    void BitMap::GetDataFromFile(const object_t* buffer, page_offset_t &offset){
        std::memcpy(&this->size, buffer + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t bytesToRead = this->HeapSize();

        this->_data = static_cast<object_t*>(std::malloc(bytesToRead));
        std::memcpy(this->_data, buffer + offset, bytesToRead);
        // for (bit_map_size_t i = 0; i < bytesToRead; i++)
        // {
        //     byte_t value;
        //     std::memcpy(&value, buffer + offset, sizeof(byte_t));
        //     this->SetByte(i, value);
        //
        //     offset += sizeof(byte_t);
        // }
    }

    void BitMap::GetDataFromFile(const object_t* buffer, page_offset_t& offset, const Int otherSize){
        this->size = otherSize;
        const bit_map_size_t bytesToRead = this->HeapSize();

        this->_data = static_cast<object_t*>(std::malloc(bytesToRead));
        std::memcpy(this->_data, buffer + offset, bytesToRead);

        offset += bytesToRead * sizeof(object_t);
    }

    void BitMap::GetDataFromFile(const std::vector<char> &buffer, page_offset_t &offset){
        std::memcpy(&this->size, buffer.data() + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t &bytesToRead = this->HeapSize();

        this->_data = static_cast<object_t*>(std::malloc(bytesToRead));
        std::memcpy(this->_data, buffer.data() + offset, bytesToRead);

        offset += bytesToRead * sizeof(object_t);
    }

    void BitMap::WriteDataToFile(std::fstream *filePtr){
        filePtr->write(reinterpret_cast<char *>(&this->size), sizeof(bit_map_size_t));
        filePtr->write(reinterpret_cast<char *>(this->_data),  this->HeapSize() * sizeof(byte_t));
    }

    void BitMap::WriteDataToFile(std::vector<char>* buffer, page_offset_t& pos)const{
        std::memcpy(buffer->data() + pos, &this->size, sizeof(bit_map_size_t));
        pos += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer->data() + pos, this->_data, heapSize * sizeof(byte_t));
        pos += heapSize * sizeof(byte_t);
    }

    void BitMap::WriteDataToBuffer(char *&buffer) const{
        std::memcpy(buffer, &this->size, sizeof(bit_map_size_t));
        buffer += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer, this->_data, heapSize);
        buffer += heapSize;
    }

    void BitMap::WriteDataToBuffer(object_t*& buffer, page_offset_t& offSet) const{
        std::memcpy(buffer + offSet, &this->size, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer + offSet, this->_data, heapSize);
        offSet += heapSize;
    }

    void BitMap::WriteDataToBuffer(std::vector<char>& buffer, page_offset_t& offSet) const {
        std::memcpy(buffer.data() + offSet, &this->size, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        const auto heapSize = this->HeapSize();

        std::memcpy(buffer.data() + offSet, this->_data, heapSize);
        offSet += heapSize;
    }

    void BitMap::Print() const{
        for (bit_map_pos_t i = 0; i < size; i++)
            std::cout << this->Get(i);

        std::cout << std::endl;
    }

    bool BitMap::Empty() const{
        return this->size == 0;
    }

    const byte_t* BitMap::DataPtr() const{
        return this->_data;
    }

    byte_t* BitMap::DataPtrUnsafe() const{
        return this->_data;
    }

    bit_map_size_t BitMap::GetSizeUnsafe() const { return this->size; }

    Int BitMap::HeapSize(const Int size){
        return (size + 7) / 8;
    }
}
