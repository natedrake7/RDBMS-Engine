#pragma once
#include "DataTypes.h"
#include "../Serialization/JsonParser.h"

namespace Serialization{
    struct JsonEntry;
}

namespace DataTypes{
    class JsonBinary final{
        const ::Memory::IAllocator* _allocator;

        object_t* _data;
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

    public:
        explicit JsonBinary(const ::Memory::IAllocator* allocator);
        explicit JsonBinary(const ::Memory::IAllocator* allocator, object_t* data, Int size);
        JsonBinary(const JsonBinary& other);
        JsonBinary(JsonBinary&& other) noexcept;
        ~JsonBinary();

        JsonBinary& operator=(const JsonBinary& other);
        JsonBinary& operator=(JsonBinary&& other) noexcept;

        [[nodiscard]] const object_t* Data() const;
        [[nodiscard]] Int Size() const;

        void SetData(object_t* data, Int size);

        Serialization::JsonValue operator[](const StringView& key) const;
    };
}
