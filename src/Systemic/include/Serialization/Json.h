#pragma once
#include <ostream>
#include "../DataStructures/PolymorphicArray.h"
#include "../DataTypes/Decimal.h"
#include "../DataStructures/SortedDictionary.h"

namespace Serialization{
    struct JsonValue;

    static constexpr char JSON_QUOTE         = '"';
    static constexpr char JSON_BACKSLASH     = '\\';
    static constexpr char JSON_SLASH         = '/';
    static constexpr char JSON_OPEN_BRACE    = '{';
    static constexpr char JSON_CLOSE_BRACE   = '}';
    static constexpr char JSON_OPEN_BRACKET  = '[';
    static constexpr char JSON_CLOSE_BRACKET = ']';
    static constexpr char JSON_COLON         = ':';
    static constexpr char JSON_COMMA         = ',';
    static constexpr char JSON_NEWLINE       = '\n';
    static constexpr char JSON_TAB           = '\t';
    static constexpr char JSON_CR            = '\r';
    static constexpr char JSON_MINUS         = '-';
    static constexpr char JSON_DOT           = '.';
    static constexpr char JSON_EXP_LOWER     = 'e';
    static constexpr char JSON_EXP_UPPER     = 'E';
    static constexpr char JSON_PLUS          = '+';
    static constexpr char JSON_TRUE_START    = 't';
    static constexpr char JSON_FALSE_START   = 'f';
    static constexpr char JSON_NULL_START    = 'n';
    static constexpr char JSON_NULL_CHAR     = '\0';

    enum class JsonType : UnsignedTinyInt{
        Null = 0,
        Bool = 1,
        Number = 2,
        String = 3,
        Array = 4,
        Object = 5,
    };

    using JsonArray = DataStructures::PolymorphicArray<JsonValue>;
    using JsonObject = SortedDictionary<DataTypes::String, JsonValue, StringComparator>;
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

        [[nodiscard]] JsonType Type() const { return _type; }

        [[nodiscard]] JsonBool          AsBool()   const { return _data._bool; }
        [[nodiscard]] const JsonNumber& AsNumber() const { return _data._number; }
        [[nodiscard]] const JsonString& AsString() const { return _data._string; }
        [[nodiscard]] const JsonArray&  AsArray()  const { return _data._array; }
        [[nodiscard]] const JsonObject& AsObject() const { return _data._object; }

        [[nodiscard]] bool IsNull()   const { return _type == JsonType::Null; }
        [[nodiscard]] bool IsBool()   const { return _type == JsonType::Bool; }
        [[nodiscard]] bool IsNumber() const { return _type == JsonType::Number; }
        [[nodiscard]] bool IsString() const { return _type == JsonType::String; }
        [[nodiscard]] bool IsArray()  const { return _type == JsonType::Array; }
        [[nodiscard]] bool IsObject() const { return _type == JsonType::Object; }
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

    class JsonWriter {
        static void PrintIndent(std::ostream& os, int indent);
        static void PrintValue(std::ostream& os, const JsonValue& value, int indent);
        static void PrintString(std::ostream& os, const JsonString& str);
    public:
        static void Print(std::ostream& os, const JsonValue& value, int indent = 0);
    };

}
