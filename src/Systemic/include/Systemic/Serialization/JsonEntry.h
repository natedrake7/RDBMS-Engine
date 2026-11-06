#pragma once
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Serialization/JsonParser.h>

namespace Serialization{
    struct JsonHeader{
        UInt32_t _size;
        UInt16_t _entryTablePosition;
        UInt8_t _type;

        static constexpr auto SIZE = sizeof(UInt32_t) + sizeof(UInt16_t) + sizeof(UInt8_t);

        [[nodiscard]] bool IsObject()const{
            return static_cast<JsonType>(this->_type) == JsonType::Object;
        }

        [[nodiscard]] bool IsArray()const{
            return static_cast<JsonType>(this->_type) == JsonType::Array;
        }

        [[nodiscard]] bool IsScalar()const{
            const auto type = static_cast<JsonType>(this->_type);
            return type != JsonType::Object && type != JsonType::Array;
        }
    };

    struct JsonEntry{
        UInt64_t _keyHash;
        UInt16_t _keyOffset;
        UInt16_t _keySize;

        UInt16_t _valueOffset;
        UInt16_t _valueSize;

        UInt8_t _type;

        explicit JsonEntry(
            const UInt64_t keyHash,
            const UInt16_t keyOffset,
            const UInt16_t keySize
        ):  _keyHash(keyHash), _keyOffset(keyOffset),
            _keySize(keySize), _valueOffset(0),
            _valueSize(0), _type(0) {}

        static constexpr auto SIZE = sizeof(UInt64_t) + 4 * sizeof(UInt16_t) + sizeof(UInt8_t);
    };

    struct JsonContainer{
        DataStructures::PolymorphicArray<JsonEntry> _entries;

        Int32_t _headerPosition;
        JsonType _type;

        bool _expectingKey = false;

        explicit JsonContainer(
            const ::Memory::IAllocator* allocator,
            const Int32_t headerPosition,
            const JsonType type,
            const bool expectingKey
        ) : _entries(allocator),
            _headerPosition(headerPosition), _type(type),
            _expectingKey(expectingKey) {}

        JsonContainer(const JsonContainer& other)
            : _entries(other._entries),
              _headerPosition(other._headerPosition),
              _type(other._type),
              _expectingKey(other._expectingKey) {}

        JsonContainer& operator=(const JsonContainer& other){
            if (this == &other)
                return *this;

            this->_entries = other._entries;
            this->_headerPosition = other._headerPosition;
            this->_type = other._type;
            this->_expectingKey = other._expectingKey;

            return *this;
        }

        JsonContainer(JsonContainer&& other) noexcept
            : _entries(std::move(other._entries)),
              _headerPosition(other._headerPosition),
              _type(other._type),
              _expectingKey(other._expectingKey) {}

        JsonContainer& operator=(JsonContainer&& other) noexcept{
            if (this == &other)
                return *this;

            this->_entries = std::move(other._entries);
            this->_headerPosition = other._headerPosition;
            this->_type = other._type;
            this->_expectingKey = other._expectingKey;

            return *this;
        }
    };
}
