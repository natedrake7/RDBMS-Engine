#pragma once
#include "../DataStructures/PolymorphicArray.h"
#include "../DataTypes/DataTypes.h"
#include "../Serialization/JsonParser.h"

namespace Serialization{
    struct JsonHeader{
        UnsignedInt _size;
        UnsignedSmallInt _entryTablePosition;
        UnsignedTinyInt _type;

        static constexpr auto SIZE = sizeof(UnsignedInt) + sizeof(UnsignedSmallInt) + sizeof(UnsignedTinyInt);

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
        UnsignedBigInt _keyHash;
        UnsignedSmallInt _keyOffset;
        UnsignedSmallInt _keySize;

        UnsignedSmallInt _valueOffset;
        UnsignedSmallInt _valueSize;

        UnsignedTinyInt _type;

        explicit JsonEntry(
            const UnsignedBigInt keyHash,
            const UnsignedSmallInt keyOffset,
            const UnsignedSmallInt keySize
        ):  _keyHash(keyHash), _keyOffset(keyOffset),
            _keySize(keySize), _valueOffset(0),
            _valueSize(0), _type(0) {}

        static constexpr auto SIZE = sizeof(UnsignedBigInt) + 4 * sizeof(UnsignedSmallInt) + sizeof(UnsignedTinyInt);
    };

    struct JsonContainer{
        DataStructures::PolymorphicArray<JsonEntry> _entries;

        Int _headerPosition;
        JsonType _type;

        bool _expectingKey = false;

        explicit JsonContainer(
            const ::Memory::IAllocator* allocator,
            const Int headerPosition,
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
