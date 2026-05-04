#include "../../include/DataTypes/JsonBinary.h"

#include "Serialization/JsonBuilder.h"

namespace DataTypes{
    bool JsonBinary::KeyEquals(
        const Serialization::JsonEntry& entry,
        const StringView& key,
        const Int headerOffSet
    ) const{
        // const auto* header = reinterpret_cast<const Serialization::JsonHeader*>(this->_data + headerOffSet);
        const auto* keyData = reinterpret_cast<const char*>(this->_data + headerOffSet + entry._keyOffset);
        return strncasecmp(keyData, key.Data(), entry._keySize) == 0;
    }

    const Serialization::JsonEntry* JsonBinary::FindEntry(
        const StringView& key,
        const Int headerOffSet
    ) const{
        const auto* header = reinterpret_cast<Serialization::JsonHeader*>(this->_data + headerOffSet);
        const auto* entries = reinterpret_cast<Serialization::JsonEntry*>(this->_data + headerOffSet + header->_entryTablePosition);

        const auto hash = std::hash<StringView>{}(key);

        Int left = 0;
        Int right = static_cast<Int>(header->_size - 1);

        while (left <= right) {
            const Int mid = left + (right - left) / 2;
            const auto& entry = entries[mid];

            if (entry._keyHash < hash){
                left = mid + 1;
                continue;
            }
            if (entry._keyHash > hash){
                right = mid - 1;
                continue;
            }

            auto i = mid;

            while (i >= 0 && entries[i]._keyHash == hash) {
                if (this->KeyEquals(entries[i], key, headerOffSet))
                    return &entries[i];
                --i;
            }

            i = mid + 1;
            while (i < header->_size && entries[i]._keyHash == hash) {
                if (this->KeyEquals(entries[i], key, headerOffSet))
                    return &entries[i];
                ++i;
            }

            return nullptr;
        }

        return nullptr; // Not found
    }

    JsonBinary::JsonBinary(const ::Memory::IAllocator* allocator)
        : _allocator(allocator), _data(nullptr), _size(0){}

    JsonBinary::JsonBinary(const ::Memory::IAllocator* allocator, object_t* data, const Int size)
        : _allocator(allocator), _data(data), _size(size){}

    JsonBinary::JsonBinary(const JsonBinary& other) = default;

    JsonBinary::JsonBinary(JsonBinary&& other) noexcept = default;

    JsonBinary::~JsonBinary() = default;

    JsonBinary& JsonBinary::operator=(const JsonBinary& other){
        if (this == &other)
            return *this;

        this->_allocator = other._allocator;
        this->_data = other._data;
        this->_size = other._size;

        return *this;
    }

    JsonBinary& JsonBinary::operator=(JsonBinary&& other) noexcept{
        if (this == &other)
            return *this;

        this->_allocator = other._allocator;
        this->_data = other._data;
        this->_size = other._size;

        other._data = nullptr;
        other._size = 0;

        return *this;
    }

    const object_t* JsonBinary::Data() const{
        return this->_data;
    }

    Int JsonBinary::Size() const{
        return this->_size;
    }

    void JsonBinary::SetData(object_t* data, const Int size){
        this->_data = data;
        this->_size = size;
    }

    Serialization::JsonValue JsonBinary::operator[](const StringView& key) const{
        auto dataType = Serialization::JsonType::Object;
        auto delimiterIndexSearch = 0;
        auto headerOffSet = 0;

        const Serialization::JsonEntry* entry = nullptr;
        while (true) {
            auto splitKey = DataTypes::String::Split(key, delimiterIndexSearch, '.');
            entry = this->FindEntry(splitKey, headerOffSet);

            if (entry == nullptr)
                return Serialization::JsonValue();

            dataType = static_cast<Serialization::JsonType>(entry->_type);
            delimiterIndexSearch = splitKey.Size() + 1;

            if (dataType != Serialization::JsonType::Object
                && dataType != Serialization::JsonType::Array
            ) break;

            headerOffSet = entry->_valueOffset;
        }

        return Serialization::JsonValue(
            this->_allocator,
            this->_data + headerOffSet + entry->_valueOffset,
            entry->_valueSize,
            dataType
        );
    }
}
