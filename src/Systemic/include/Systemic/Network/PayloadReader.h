#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>
#include <string>


namespace Network{
    class PayloadReader{
        const char* _data;
        UInt32_t _size;
        UInt32_t _offset;

        [[nodiscard]] bool BoundsCheck(const UInt32_t dataSize) const{
            // Subtraction, not addition: _offset + dataSize wraps for a hostile length.
            return dataSize <= this->_size - this->_offset;
        }

        public:
            PayloadReader(const char* data, const UInt32_t size)
                : _data(data), _size(size), _offset(0){}

            template<typename T> requires std::is_trivially_copyable_v<T>
            [[nodiscard]] bool Read(T& value){
                if (!this->BoundsCheck(sizeof(T)))
                    return false;

                std::memcpy(&value, this->_data + this->_offset, sizeof(T));
                this->_offset += sizeof(T);
                return true;
            }

            [[nodiscard]] bool ReadSpan(const char*& data, const UInt64_t size){
                if (!this->BoundsCheck(size))
                    return false;

                data = this->_data + this->_offset;
                this->_offset += size;
                return true;
            }

            [[nodiscard]] bool ReadUnsignedInt(UInt32_t& value){
                if (!this->BoundsCheck(sizeof(UInt32_t)))
                    return false;

                std::memcpy(&value, this->_data + this->_offset, sizeof(UInt32_t));
                this->_offset += sizeof(UInt32_t);
                return true;
            }

            [[nodiscard]] bool ReadString(std::string& value){
                UInt32_t length = 0;
                if (!this->ReadUnsignedInt(length))
                    return false;

                if (!this->BoundsCheck(length))
                    return false;

                value.assign(this->_data + this->_offset, length);
                this->_offset += length;
                return true;
            }

            [[nodiscard]] bool ReadStringView(DataTypes::StringView& value){
                UInt32_t length = 0;
                if (!this->ReadUnsignedInt(length))
                    return false;

                if (!this->BoundsCheck(length))
                    return false;

                value = DataTypes::StringView(this->_data + this->_offset, length);
                this->_offset += length;
                return true;
            }


    };
}