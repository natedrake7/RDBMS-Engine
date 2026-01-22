#include "../../include/DataStructures/BitMap.h"
#include <cstring>
#include <iostream>

#include <fstream>

namespace ByteMaps
{
    BitMap::BitMap(){
        this->size = 0;
    }

    BitMap::BitMap(const BitMap &bitMap){
        this->size = bitMap.size;
        this->data = bitMap.data;
    }

    BitMap::BitMap(const BitMap *bitMap){
        this->size = bitMap->size;
        this->data = bitMap->data;
    }

    BitMap::BitMap(const bit_map_size_t size, const byte_t defaultValue){
        this->size = size;
        this->data.resize((size + 7) / 8, defaultValue);
    }

    BitMap::~BitMap() = default;

    void BitMap::Set(const bit_map_pos_t position, const bool value){
        if (position >= this->size)
            this->Resize(position + 1);

        if (value)
        {
            data[position / 8] |= (1 << (position % 8)); // Set the bit
            return;
        }

        data[position / 8] &= ~(1 << (position % 8)); // Clear the bit
    }

    bool BitMap::Get(const bit_map_pos_t position) const { return data[position / 8] & (1 << (position % 8)); }

    bit_map_size_t BitMap::GetSize() const { return this->size; }

    bit_map_size_t BitMap::GetSizeInBytes() const { return this->data.size() + sizeof(bit_map_size_t); }

    void BitMap::SetByte(const bit_map_pos_t position, const byte_t value){
        if (position < this->data.size())
            data[position] = value;
    }

    void BitMap::GetDataFromFile(const object_t*& buffer, page_offset_t &offset){
        std::memcpy(&this->size, buffer + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t &bytesToRead = (this->size + 7) / 8;

        if (this->data.empty())
            this->data.resize(bytesToRead);

        for (bit_map_size_t i = 0; i < bytesToRead; i++)
        {
            byte_t value;
            std::memcpy(&value, buffer + offset, sizeof(byte_t));
            this->SetByte(i, value);

            offset += sizeof(byte_t);
        }
    }

    void BitMap::GetDataFromFile(const std::vector<char> &buffer, page_offset_t &offset){
        std::memcpy(&this->size, buffer.data() + offset, sizeof(bit_map_size_t));
        offset += sizeof(bit_map_size_t);

        const bit_map_size_t &bytesToRead = (this->size + 7) / 8;

        if (this->data.empty())
            this->data.resize(bytesToRead);

        for (bit_map_size_t i = 0; i < bytesToRead; i++)
        {
            byte_t value;
            std::memcpy(&value, buffer.data() + offset, sizeof(byte_t));
            this->SetByte(i, value);

            offset += sizeof(byte_t);
        }
    }

    void BitMap::WriteDataToFile(std::fstream *filePtr){
        filePtr->write(reinterpret_cast<char *>(&this->size), sizeof(bit_map_size_t));
        filePtr->write(reinterpret_cast<char *>(this->data.data()), this->data.size() * sizeof(byte_t));
    }

    void BitMap::WriteDataToFile(std::vector<char>* buffer, page_offset_t& pos)const{
        std::memcpy(buffer->data() + pos, &this->size, sizeof(bit_map_size_t));
        pos += sizeof(bit_map_size_t);

        std::memcpy(buffer->data() + pos, this->data.data(), this->data.size() * sizeof(byte_t));
        pos += this->data.size() * sizeof(byte_t);
    }

    void BitMap::WriteDataToBuffer(char *&buffer) const{
        std::memcpy(buffer, &this->size, sizeof(bit_map_size_t));
        buffer += sizeof(bit_map_size_t);

        const int dataSize = this->data.size() * sizeof(byte_t);
        
        std::memcpy(buffer, this->data.data(), dataSize);
        buffer += dataSize;
    }

    void BitMap::WriteDataToBuffer(object_t*& buffer, page_offset_t& offSet) const{
        std::memcpy(buffer + offSet, &this->size, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        const int dataSize = this->data.size() * sizeof(byte_t);

        std::memcpy(buffer + offSet, this->data.data(), dataSize);
        offSet += dataSize;
    }

    void BitMap::WriteDataToBuffer(std::vector<char>& buffer, page_offset_t& offSet) const {
        std::memcpy(buffer.data() + offSet, &this->size, sizeof(bit_map_size_t));
        offSet += sizeof(bit_map_size_t);

        const int dataSize = this->data.size() * sizeof(byte_t);

        std::memcpy(buffer.data() + offSet, this->data.data(), dataSize);
        offSet += dataSize;
    }

    void BitMap::Print() const{
        for (bit_map_pos_t i = 0; i < size; i++)
            std::cout << this->Get(i);

        std::cout << std::endl;
    }

    bool BitMap::Empty() const{
        return this->size == 0;
    }

    const std::vector<byte_t>& BitMap::GetData() const { return this->data; }

    std::vector<byte_t>& BitMap::GetDataUnsafe(){ return this->data; }

    bit_map_size_t BitMap::GetSizeUnsafe() const { return this->size; }

    BitMap &BitMap::operator=(const BitMap &bitMap){
        if (&bitMap == this)
            return *this;

        this->data = bitMap.GetData();
        this->size = bitMap.GetSize();

        return *this;
    }

    void BitMap::Resize(const bit_map_size_t &newSize){
        const uint16_t newByteCount = (newSize + 7) / 8;
        this->data.resize(newByteCount, 0);
        this->size = newSize;
    }
}
