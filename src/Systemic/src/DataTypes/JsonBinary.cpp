#include "../../include/DataTypes/JsonBinary.h"

#include "Comparators.h"
#include "DataTypes/StringValue.h"
#include "Serialization/JsonBuilder.h"

namespace DataTypes{
    // JsonKey::JsonKey()
    //     : _data(String::Null()), _type(JsonKeyType::Key){}
    //
    // JsonKey::JsonKey(const JsonKey& other){
    //     this->_data = other._data;
    // }
    //
    // JsonKey& JsonKey::operator=(const JsonKey& other){
    //     if (this == &other)
    //         return *this;
    //
    //     this->Copy(other);
    //     return *this;
    // }
    //
    // JsonKey::JsonKey(JsonKey&& other) noexcept{
    //     this->Move(std::move(other));
    // }
    //
    // JsonKey& JsonKey::operator=(JsonKey&& other) noexcept{
    //     if (this == &other)
    //         return *this;
    //
    //     this->Move(std::move(other));
    //     return *this;
    // }

    JsonPathStep::JsonPathStep()
        : _key(String::Null()), _accessorType(JsonAccessorType::Json){}

    JsonPathStep::JsonPathStep(String&& key, const JsonAccessorType accessorType)
        : _key(std::move(key)), _accessorType(accessorType){}

    JsonPathStep::JsonPathStep(const JsonPathStep& other){
        this->_key = other._key;
        this->_accessorType = other._accessorType;
    }

    JsonPathStep& JsonPathStep::operator=(const JsonPathStep& other){
        if (this == &other)
            return *this;

        this->_key = other._key;
        this->_accessorType = other._accessorType;
        return *this;
    }

    JsonPathStep::JsonPathStep(JsonPathStep&& other) noexcept{
        this->_key = std::move(other._key);
        this->_accessorType = other._accessorType;
    }

    JsonPathStep& JsonPathStep::operator=(JsonPathStep&& other) noexcept{
        if (this == &other)
            return *this;

        this->_key = std::move(other._key);
        this->_accessorType = other._accessorType;
        return *this;
    }

    bool JsonBinary::KeyEquals(
        const Serialization::JsonEntry& entry,
        const StringView& key,
        const Int headerOffSet
    ) const{
        const auto* keyData = reinterpret_cast<const char*>(this->_data + headerOffSet + entry._keyOffset);
        return Comparators::CompareIgnoreOrdinalCase(keyData, key.Data(), entry._keySize) == Comparators::Comparator::Equal;
    }

    const Serialization::JsonEntry* JsonBinary::FindEntry(
        const StringView& key,
        const Int headerOffSet
    ) const{
        const auto* header = reinterpret_cast<const Serialization::JsonHeader*>(this->_data + headerOffSet);
        const auto* entries = reinterpret_cast<const Serialization::JsonEntry*>(this->_data + headerOffSet + header->_entryTablePosition);

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

    void JsonBinary::SerializeNode(String& str, const Int headerOffset)const{
        auto* header = reinterpret_cast<const Serialization::JsonHeader*>(this->_data + headerOffset);
        auto* entries = reinterpret_cast<const Serialization::JsonEntry*>(this->_data + headerOffset + header->_entryTablePosition);

        if (header->IsObject()) {
            str.Append('{');
            for (UnsignedInt i = 0; i < header->_size; i++) {
                const auto& entry = entries[i];
                if (i > 0) str.Append(',');

                // key
                str.Append('"');
                str.Append(
                    reinterpret_cast<const char*>(this->_data + headerOffset + entry._keyOffset),
                    entry._keySize
                );
                str.Append('"');
                str.Append(':');

                // value
                this->SerializeValue(str, headerOffset, entry);
            }
            str.Append('}');
            return;
        }

        if (header->IsArray()) {
            str.Append('[');
            for (UnsignedInt i = 0; i < header->_size; i++) {
                if (i > 0) str.Append(',');
                this->SerializeValue(str, headerOffset, entries[i]);
            }
            str.Append(']');
        }
    }

    void JsonBinary::SerializeValue(
        String& result,
        const Int headerOffset,
        const Serialization::JsonEntry& entry
    ) const{
        const auto type = static_cast<Serialization::JsonType>(entry._type);
        const auto* valuePtr = this->_data + headerOffset + entry._valueOffset;

        switch (type) {
        case Serialization::JsonType::Null:
            result.Append("null", 4);
            break;
        case Serialization::JsonType::Bool:
            result.Append(*reinterpret_cast<const bool*>(valuePtr) ? "true" : "false");
            break;
        case Serialization::JsonType::String:
            result.Append('"');
            result.Append(reinterpret_cast<const char*>(valuePtr), entry._valueSize);
            result.Append('"');
            break;
        case Serialization::JsonType::Number: {
                const auto decimal = DataTypes::Decimal(valuePtr, entry._valueSize);
                const auto str = decimal.ToString(this->_allocator);
                result.Append(str.Data(), str.Size());
                break;
        }
        case Serialization::JsonType::Object:
        case Serialization::JsonType::Array:
            // valueOffset points to the nested header — recurse
            this->SerializeNode(result, headerOffset + entry._valueOffset);
            break;
        }
    }

    JsonBinary::JsonBinary()
        : _allocator(nullptr), _data(nullptr), _size(0){}

    JsonBinary::JsonBinary(const ::Memory::IAllocator* allocator)
        : _allocator(allocator), _data(nullptr), _size(0){}

    JsonBinary::JsonBinary(const ::Memory::IAllocator* allocator, const object_t* data, const Int size)
        : _allocator(allocator), _data(data), _size(size){}

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

    void JsonBinary::SetData(const object_t* data, const Int size){
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

            headerOffSet += entry->_valueOffset;
        }

        return Serialization::JsonValue(
            this->_allocator,
            this->_data + headerOffSet + entry->_valueOffset,
            entry->_valueSize,
            dataType
        );
    }

    Serialization::JsonValue JsonBinary::Navigate(
        const DataStructures::PolymorphicArray<JsonPathStep>& pathSegments
    ) const{
        auto dataType = Serialization::JsonType::Object;
        auto headerOffSet = 0;

        const Serialization::JsonEntry* entry = nullptr;

        const auto segmentsSize = pathSegments.Size();
        for (int i = 0; i < segmentsSize; i++){
            entry = this->FindEntry(StringView::ViewOf(pathSegments[i]._key), headerOffSet);

            if (entry == nullptr)
                return Serialization::JsonValue();

            dataType = static_cast<Serialization::JsonType>(entry->_type);

            //less than array is primitive or null
            if (dataType < Serialization::JsonType::Array)
                continue;

            if (i < segmentsSize - 1)
                headerOffSet += entry->_valueOffset;
        }

        return Serialization::JsonValue(
            this->_allocator,
            this->_data + headerOffSet + entry->_valueOffset,
            entry->_valueSize,
            dataType
        );
    }

    String JsonBinary::ToString() const{
        if (this->_data == nullptr || this->_size == 0)
            return String::Empty(this->_allocator);

        String result(this->_allocator);
        this->SerializeNode(result, 0);
        return result;
    }

    StringValue JsonBinary::ToStringValue() const{
        if (this->_data == nullptr || this->_size == 0)
            return StringValue::Empty();

        String result(this->_allocator, this->_size);
        this->SerializeNode(result, 0);
        return StringValue::MoveFromString(result);
    }

    String JsonBinary::JsonObjectToString(
        const Serialization::JsonValue& value,
        const ::Memory::IAllocator* allocator
    ){
        const JsonBinary jsonBinary(allocator, static_cast<const object_t*>(value.Data()), value.Size());
        return jsonBinary.ToString();
    }
}
