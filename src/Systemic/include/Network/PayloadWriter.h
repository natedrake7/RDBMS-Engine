#pragma once
#include "../DataTypes/DataTypes.h"
#include <vector>
#include <string>

namespace Network{
    class PayloadWriter{
        std::vector<char>* _buffer;

        public:
            explicit PayloadWriter(std::vector<char>* buffer)
                : _buffer(buffer){}

            void WriteUnsignedInt(const UnsignedInt value) const{
                const auto* bytes = reinterpret_cast<const char*>(&value);
                this->_buffer->insert(this->_buffer->end(), bytes, bytes + sizeof(UnsignedInt));
            }

            void WriteString(const std::string& value) const{
                this->WriteUnsignedInt(value.size());
                this->_buffer->insert(this->_buffer->end(), value.begin(), value.end());
            }
    };
}