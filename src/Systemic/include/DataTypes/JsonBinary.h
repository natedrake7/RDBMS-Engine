#pragma once
#include "DataTypes.h"
#include "../Serialization/JsonParser.h"

namespace Serialization{
    struct JsonEntry;
}

namespace DataTypes{
    enum class JsonAccessorType: UnsignedTinyInt{
        Json = 0,
        Scalar = 1
    };

    enum class JsonKeyType : UnsignedTinyInt{
        Key = 0,
        Index = 1
    };

    struct JsonKey{
        union Data{
            String _key;
            Int _arrayIndex;

            Data();
            ~Data();
        } _data;

        JsonKeyType _type;

        void Copy(const JsonKey& other);
        void Move(JsonKey&& other) noexcept;

        JsonKey();
        JsonKey(const JsonKey& other);
        JsonKey& operator=(const JsonKey& other);

        JsonKey(JsonKey&& other) noexcept;
        JsonKey& operator=(JsonKey&& other) noexcept;
    };

    struct JsonPathStep{
        JsonKey _key;
        JsonAccessorType _accessorType;

        JsonPathStep();
        JsonPathStep(JsonKey&& key, JsonAccessorType accessorType);
        JsonPathStep(const JsonPathStep& other);
        JsonPathStep& operator=(const JsonPathStep& other);
        JsonPathStep(JsonPathStep&& other) noexcept;
        JsonPathStep& operator=(JsonPathStep&& other) noexcept;
    };

    class JsonBinary final{
        const ::Memory::IAllocator* _allocator;
        const object_t* _data;
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
        explicit JsonBinary(const ::Memory::IAllocator* allocator, const object_t* data, Int size);
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
