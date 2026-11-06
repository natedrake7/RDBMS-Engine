#pragma once
#include <Systemic/Serialization/JsonParser.h>
#include <Systemic/DataTypes/JsonBinary.h>
#include <Systemic/Serialization/JsonEntry.h>

#include <Systemic/DataStructures/PolymorphicArray.h>

namespace Serialization{
    struct JsonContainer;

    class JsonBuilder{
        DataStructures::PolymorphicArray<object_t> _buffer;
        DataStructures::PolymorphicArray<JsonContainer> _containers;

        const ::Memory::IAllocator* _allocator;

        [[nodiscard]] static Int32_t RelativeOffSet(const Int32_t offset, const Int32_t headerPosition){
            return offset - headerPosition;
        }

        inline void Value(const void* data, Int32_t size, JsonType type);

        inline void SetObjectSize(Int32_t size);

    public:
        explicit JsonBuilder(const ::Memory::IAllocator* allocator);

        void StartObject();
        void EndObject();

        void StartArray();
        void EndArray();

        void Key(const DataTypes::StringView& key);
        void Value(const DataTypes::String& value);
        void Value(bool value);
        void Value(const DataTypes::Decimal& value);
        void ValueNull();

        [[nodiscard]] DataTypes::JsonBinary Build();
    };
}
