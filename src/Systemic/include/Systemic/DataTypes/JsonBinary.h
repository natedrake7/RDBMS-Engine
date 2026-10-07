#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/Serialization/JsonParser.h>

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
        data_size_t _size;

        [[nodiscard]] bool KeyEquals(
            const Serialization::JsonEntry& entry,
            const StringView& key,
            data_size_t headerOffSet
        ) const;
        [[nodiscard]] const Serialization::JsonEntry* FindEntry(
            const StringView& key,
            data_size_t headerOffSet
        ) const;

        void SerializeNode(String& str, data_size_t headerOffset)const;
        void SerializeValue(
            String& result,
            data_size_t headerOffset,
            const Serialization::JsonEntry& entry
        )const;

    public:
        JsonBinary();
        explicit JsonBinary(const ::Memory::IAllocator* allocator);
        explicit JsonBinary(const ::Memory::IAllocator* allocator, const object_t* data, data_size_t size);

        JsonBinary& operator=(const JsonBinary& other);
        JsonBinary& operator=(JsonBinary&& other) noexcept;

        [[nodiscard]] const object_t* Data() const;
        [[nodiscard]] data_size_t Size() const;

        void SetData(const object_t* data, data_size_t size);

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
