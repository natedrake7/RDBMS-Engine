#pragma once
#include "../DataStructures/PolymorphicArray.h"
#include "../DataStructures/Dictionary.h"
#include "../DataTypes/Decimal.h"

namespace Serialization{
    struct JsonValue;

    enum class JsonType : UnsignedTinyInt{
        Null = 0,
        Bool = 1,
        Number = 2,
        String = 3,
        Array = 4,
        Object = 5,
    };

    using JsonArray = DataStructures::PolymorphicArray<JsonValue>;
    using JsonObject = Dictionary<DataTypes::String, JsonValue>;
    using JsonString = DataTypes::String;
    using JsonNumber = DataTypes::Decimal;
    using JsonBool = bool;
    using JsonNull = decltype(nullptr);

    struct JsonValue{
    private:
        union Data{
            JsonBool _bool;
            JsonNumber _number;
            JsonString _string;
            JsonArray _array;
            JsonObject _object;

            Data();
            ~Data();
        } _data;
        JsonType _type;

        void Copy(const JsonValue& other);
        void Move(JsonValue&& other) noexcept;
    public:

        JsonValue();

        JsonValue(const JsonValue& other);
        JsonValue(JsonValue&& other) noexcept;
        JsonValue& operator=(JsonValue&& other) noexcept;
        JsonValue& operator=(const JsonValue& other);
        ~JsonValue();

        explicit JsonValue(JsonNull);
        explicit JsonValue(JsonBool value);
        explicit JsonValue(const JsonNumber& value);
        explicit JsonValue(JsonString&& value);
        explicit JsonValue(JsonArray&& value);
        explicit JsonValue(JsonObject&& value);
    };

    class JsonParser{
        const ::Memory::IAllocator* _allocator;
        DataTypes::StringView _src;
        Int _pos;

            void SkipWhitespace();
            void SkipComment();

            [[nodiscard]] char Peek() const;
            char Consume();

            [[nodiscard]] JsonValue ParseValue();
            [[nodiscard]] JsonString ParseString();
            [[nodiscard]] JsonNumber ParseNumber();
            [[nodiscard]] JsonBool ParseBool();
            [[nodiscard]] JsonNull ParseNull();
            [[nodiscard]] JsonArray ParseArray();
            [[nodiscard]] JsonObject ParseObject();

        public:
            explicit JsonParser(const ::Memory::IAllocator* allocator, const char* src);
            explicit JsonParser(const ::Memory::IAllocator* allocator, const DataTypes::String& src);
            explicit JsonParser(const ::Memory::IAllocator* allocator, const std::string& src);
            explicit JsonParser(const ::Memory::IAllocator* allocator, DataTypes::StringView&&  src);

            JsonValue Parse();
    };

}
