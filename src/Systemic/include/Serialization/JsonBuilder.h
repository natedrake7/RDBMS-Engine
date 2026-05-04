#pragma once
#include "JsonParser.h"
#include "../DataTypes/JsonBinary.h"
#include "JsonEntry.h"

#include "../DataStructures/PolymorphicArray.h"

namespace Serialization{
    struct JsonContainer;

    class JsonBuilder{
        DataStructures::PolymorphicArray<object_t> _buffer;
        DataStructures::PolymorphicArray<JsonContainer> _containers;

        const ::Memory::IAllocator* _allocator;

        [[nodiscard]] static Int RelativeOffSet(const Int offset, const Int headerPosition){
            return offset - headerPosition;
        }

    public:
        explicit JsonBuilder(const ::Memory::IAllocator* allocator);

        void StartObject();
        void EndObject();

        void StartArray();
        void EndArray();

        void Key(const DataTypes::StringView& key);
        void Value(const JsonValue& value);

        [[nodiscard]] DataTypes::JsonBinary Build();
    };
}
