#pragma once
#include "DataTypes.h"
#include "String.h"
#include "../Serialization/JsonParser.h"

namespace Serialization{
    struct JsonEntry;
}

namespace DataTypes{
    enum class JsonAccessorType: UnsignedTinyInt{
        Json = 0,
        Scalar = 1
    };

    struct JsonPathStep{
        String _key;
        JsonAccessorType _accessorType;

        JsonPathStep();
        JsonPathStep(String&& key, JsonAccessorType accessorType);
        JsonPathStep(const JsonPathStep& other);
        JsonPathStep& operator=(const JsonPathStep& other);
        JsonPathStep(JsonPathStep&& other) noexcept;
        JsonPathStep& operator=(JsonPathStep&& other) noexcept;
    };

    class JsonBinary final{
        const object_t* _data;
        const ::Memory::IAllocator* _allocator;
        Int _size;

        [[nodiscard]] bool KeyEquals(
            const Serialization::JsonEntry& entry,
            const StringView& key,
            Int headerOffSet
        ) const;
        [[nodiscard]] const Serialization::JsonEntry* FindEntry(
            const StringView& key,
            Int headerOffSet
        ) const;

        void SerializeNode(String& str, Int headerOffset)const;
        void SerializeValue(
            String& result,
            Int headerOffset,
            const Serialization::JsonEntry& entry
        )const;

    public:
        JsonBinary();
        explicit JsonBinary(const ::Memory::IAllocator* allocator);
        explicit JsonBinary(const ::Memory::IAllocator* allocator, const object_t* data, Int size);

        JsonBinary& operator=(const JsonBinary& other);
        JsonBinary& operator=(JsonBinary&& other) noexcept;

        [[nodiscard]] const object_t* Data() const;
        [[nodiscard]] Int Size() const;

        void SetData(const object_t* data, Int size);

        Serialization::JsonValue operator[](const StringView& key) const;
        [[nodiscard]] Serialization::JsonValue Navigate(const DataStructures::PolymorphicArray<JsonPathStep>& pathSegments) const;

        [[nodiscard]] String ToString() const;
        [[nodiscard]] StringValue ToStringValue() const;
        [[nodiscard]] static String JsonObjectToString(
            const Serialization::JsonValue& value,
            const ::Memory::IAllocator* allocator
        );
    };
}
