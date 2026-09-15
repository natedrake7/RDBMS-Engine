#pragma once
#include "../DataTypes/DataTypes.h"
#include "../DataTypes/StringView.h"
#include <string>


namespace Network{
    class PayloadReader{
        const char* _data;
        UnsignedInt _size;
        UnsignedInt _offset;

        [[nodiscard]] bool BoundsCheck(const UnsignedInt dataSize) const{
            // Subtraction, not addition: _offset + dataSize wraps for a hostile length.
            return dataSize <= this->_size - this->_offset;
        }

        public:
            PayloadReader(const char* data, const UnsignedInt size)
                : _data(data), _size(size), _offset(0){}

            [[nodiscard]] bool ReadUnsignedInt(UnsignedInt& value){
                if (!this->BoundsCheck(sizeof(UnsignedInt)))
                    return false;

                std::memcpy(&value, this->_data + this->_offset, sizeof(UnsignedInt));
                this->_offset += sizeof(UnsignedInt);
                return true;
            }

            [[nodiscard]] bool ReadString(std::string& value){
                UnsignedInt length = 0;
                if (!this->ReadUnsignedInt(length))
                    return false;

                if (!this->BoundsCheck(length))
                    return false;

                value.assign(this->_data + this->_offset, length);
                this->_offset += length;
                return true;
            }

            [[nodiscard]] bool ReadStringView(DataTypes::StringView& value){
                UnsignedInt length = 0;
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