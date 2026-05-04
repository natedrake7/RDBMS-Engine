#include <utility>

#include "../include/Serialization/JsonParser.h"

namespace Serialization {
    JsonValue::Data::Data()
        : _bool(false){}

    JsonValue::Data::~Data(){}

    void JsonValue::Copy(const JsonValue& other){
        this->_type = other._type;
        switch (this->_type) {
        case JsonType::Bool:
            this->_data._bool = other._data._bool;
            break;
        case JsonType::Number:
            this->_data._number = other._data._number;
            break;
        case JsonType::String:
            this->_data._string = other._data._string;
            break;
        case JsonType::Array:
            this->_data._array = other._data._array;
            break;
        case JsonType::Object:
            this->_data._object = other._data._object;
            break;
        default:
            break;
        }
    }

    void JsonValue::Move(JsonValue&& other) noexcept{
        this->_type = other._type;
        switch (this->_type) {
        case JsonType::Bool:
            this->_data._bool = other._data._bool;
            break;
        case JsonType::Number:
            this->_data._number = other._data._number;
            break;
        case JsonType::String:
            this->_data._string = std::move(other._data._string);
            break;
        case JsonType::Array:
            this->_data._array = std::move(other._data._array);
            break;
        case JsonType::Object:
            new (&this->_data._object) JsonObject(std::move(other._data._object));
            break;
        default:
            break;
        }
        other._type = JsonType::Null;
    }

    JsonValue::JsonValue() : _type(JsonType::Null) {}

    JsonValue::JsonValue(const JsonValue& other): _type(other._type) {
        this->Copy(other);
    }

    JsonValue::JsonValue(JsonValue&& other) noexcept : _type(other._type) {
        this->Move(std::move(other));
    }

    JsonValue& JsonValue::operator=(JsonValue&& other) noexcept {
        if (this == &other) return *this;
        this->Move(std::move(other));
        return *this;
    }

    JsonValue& JsonValue::operator=(const JsonValue& other) {
        if (this == &other) return *this;
        this->Copy(other);
        return *this;
    }

    JsonValue::~JsonValue() = default;

    JsonValue::JsonValue(JsonNull)
        : _type(JsonType::Null){}

    JsonValue::JsonValue(const JsonBool value) : _type(JsonType::Bool) {
        _data._bool = value;
    }

    JsonValue::JsonValue(const JsonNumber& value) : _type(JsonType::Number) {
        this->_data._number = value;
    }

    JsonValue::JsonValue(JsonString&& value) : _type(JsonType::String) {
        this->_data._string = std::move(value);
    }

    JsonValue::JsonValue(JsonArray&& value) : _type(JsonType::Array) {
        this->_data._array = std::move(value);
    }

    JsonValue::JsonValue(JsonObject&& value) : _type(JsonType::Object) {
        new (&this->_data._object) JsonObject(std::move(value));
    }

    JsonValue::JsonValue(
        const ::Memory::IAllocator* allocator,
        const object_t* data, const Int size,
        const JsonType type
    )
    {
        this->_type = type;
        switch (type) {
        case JsonType::Bool:
            this->_data._bool = *reinterpret_cast<const JsonBool*>(data);
            break;
        case JsonType::Number:
            this->_data._number = DataTypes::Decimal(data, size);
            break;
        case JsonType::String:
            this->_data._string = DataTypes::String(data, size, allocator);
            break;
        default:
            break;
        }
    }

    Int JsonValue::Size() const{
        switch (this->_type) {
        case JsonType::Bool:
            return 1;
        case JsonType::Number:
            return this->_data._number.GetRawDataSize();
        case JsonType::String:
            return this->_data._string.Size();
        case JsonType::Null:
            return 0;
        case JsonType::Array:
            return this->_data._array.Size();
        case JsonType::Object:
            return static_cast<Int>(this->_data._object.size());
        }
    }

    const void* JsonValue::Data() const{
        switch (this->_type) {
            case JsonType::Bool: return &this->_data._bool;
            case JsonType::Number: return &this->_data._number.Data();
            case JsonType::String: return this->_data._string.Data();
            case JsonType::Null: return nullptr;
            case JsonType::Array: return this->_data._array.Data();
            case JsonType::Object: return &this->_data._object;
        }
    }

    void JsonParser::SkipWhitespace(){
        while (this->_pos < this->_src.Size() && std::isspace(this->_src[this->_pos]))
            this->_pos++;
    }

    void JsonParser::SkipComment(){
        if (this->_src.Size() < this->_pos + 2)
            return;

        if (this->_src[this->_pos] == JSON_SLASH && this->_src[this->_pos + 1] == JSON_SLASH) {
            while (this->_pos < this->_src.Size() && this->_src[this->_pos] != JSON_NEWLINE)
                this->_pos++;
        }
    }

    char JsonParser::Peek() const{
        return this->_pos < this->_src.Size() ? this->_src[this->_pos] : JSON_NULL_CHAR;
    }

    char JsonParser::Consume(){
        return this->_src[this->_pos++];
    }

    JsonValue JsonParser::ParseValue(){
        this->SkipWhitespace();
        this->SkipComment();

        const auto character = this->Peek();
        if (character == JSON_QUOTE)
            return JsonValue(this->ParseString());
        if (character == JSON_OPEN_BRACE)
            return JsonValue(this->ParseObject());
        if (character == JSON_OPEN_BRACKET)
            return JsonValue(this->ParseArray());
        if (character == JSON_TRUE_START || character == JSON_FALSE_START)
            return JsonValue(this->ParseBool());
        if (character == JSON_NULL_START)
            return JsonValue(this->ParseNull());
        if (character == JSON_MINUS || std::isdigit(character))
            return JsonValue(this->ParseNumber());
        throw std::runtime_error(std::string("Unexpected character: ") + character);
    }

    JsonString JsonParser::ParseString(){
        this->Consume(); // opening "

        DataTypes::String result(this->_allocator);

        const auto* base = this->_src.Data();
        const auto size = this->_src.Size();

        while (this->_pos < size && base[this->_pos] != JSON_QUOTE) {
            if (base[this->_pos] != JSON_BACKSLASH) {
                const Int start = this->_pos;
                while (this->_pos < size && base[this->_pos] != JSON_QUOTE && base[this->_pos] != JSON_BACKSLASH)
                    ++this->_pos;
                result.Append(base + start, this->_pos - start);
                continue;
            }

            ++this->_pos; // skip backslash
            switch (this->_src[this->_pos++]) {
                case JSON_QUOTE:     result.Append(JSON_QUOTE);     break;
                case JSON_BACKSLASH: result.Append(JSON_BACKSLASH); break;
                case JSON_SLASH:     result.Append(JSON_SLASH);     break;
                case 'n':            result.Append(JSON_NEWLINE);   break;
                case 't':            result.Append(JSON_TAB);       break;
                case 'r':            result.Append(JSON_CR);        break;
                default: throw std::runtime_error("JsonParser::ParseString: Invalid escape sequence. Error at position: " + std::to_string(this->_pos));
            }
        }

        if (this->_pos >= size || base[this->_pos] != JSON_QUOTE)
            throw std::runtime_error("JsonParser::ParseString: Unterminated string. Error at position: " + std::to_string(this->_pos));
        ++this->_pos; // closing "

        return result;
    }

    JsonNumber JsonParser::ParseNumber() {
        const auto start = this->_pos;

        if (this->Peek() == JSON_MINUS)
            this->Consume();

        while (std::isdigit(this->Peek()))
            this->Consume();

        // Decimal part
        if (this->Peek() == JSON_DOT) {
            this->Consume();
            while (std::isdigit(this->Peek()))
                this->Consume();
        }

        // Exponent part
        if (this->Peek() == JSON_EXP_LOWER || this->Peek() == JSON_EXP_UPPER) {
            this->Consume();
            if (this->Peek() == JSON_PLUS || this->Peek() == JSON_MINUS)
                this->Consume();
            while (std::isdigit(this->Peek()))
                this->Consume();
        }

        const auto view = DataTypes::StringView(this->_src.Data() + start, this->_pos - start);
        return JsonNumber(view);
    }

    JsonBool JsonParser::ParseBool(){
        const char* p = this->_src.Data() + this->_pos;
        if (std::memcmp(p, "true", 4) == 0)  { this->_pos += 4; return true; }
        if (std::memcmp(p, "false", 5) == 0) { this->_pos += 5; return false; }
        throw std::runtime_error("JsonParser::ParseBool: Invalid boolean value. Error at position: " + std::to_string(this->_pos));
    }

    JsonNull JsonParser::ParseNull(){
        if (std::memcmp(this->_src.Data() + this->_pos, "null", 4) == 0) {
            this->_pos += 4;
            return nullptr;
        }
        throw std::runtime_error("JsonParser::ParseNull: Invalid null value. Error at position: " + std::to_string(this->_pos));
    }

    JsonArray JsonParser::ParseArray(){
        this->Consume(); // opening [

        JsonArray array(this->_allocator);
        this->SkipWhitespace();
        if (this->Peek() == JSON_CLOSE_BRACKET) {
            this->Consume();
            return array;
        }

        while (true) {
            array.Push(this->ParseValue());
            this->SkipWhitespace();
            if (this->Peek() == JSON_CLOSE_BRACKET) {
                this->Consume();
                break;
            }
            if (this->Peek() != JSON_COMMA)
                throw std::runtime_error("JsonParser::ParseArray: Expected ',' in array. Error at position: " + std::to_string(this->_pos));
            this->Consume();
        }

        return array;
    }

    JsonObject JsonParser::ParseObject(){
        this->Consume(); // opening {

        JsonObject object;
        this->SkipWhitespace();
        if (this->Peek() == JSON_CLOSE_BRACE) {
            this->Consume();
            return object;
        }

        while (true) {
            this->SkipWhitespace();

            if (this->Peek() != JSON_QUOTE)
                throw std::runtime_error("JsonParser::ParseObject: Expected string key. Error at position: " + std::to_string(this->_pos));

            auto key = this->ParseString();
            this->SkipWhitespace();
            if (this->Consume() != JSON_COLON)
                throw std::runtime_error("JsonParser::ParseObject: Expected ':' in object. Error at position: " + std::to_string(this->_pos));

            object[key] = this->ParseValue();
            this->SkipWhitespace();

            if (this->Peek() == JSON_CLOSE_BRACE) {
                this->Consume();
                break;
            }
            if (this->Peek() != JSON_COMMA)
                throw std::runtime_error("JsonParser::ParseObject: Expected ',' in object. Error at position: " + std::to_string(this->_pos));
            this->Consume();
        }

        return object;
    }

    JsonParser::JsonParser(const ::Memory::IAllocator* allocator, const char* src)
        : _allocator(allocator), _src(src, static_cast<Int>(std::strlen(src))), _pos(0){}

    JsonParser::JsonParser(const ::Memory::IAllocator* allocator, const DataTypes::String& src)
        : _allocator(allocator), _src(src.Data(), src.Size()), _pos(0){}

    JsonParser::JsonParser(const ::Memory::IAllocator* allocator, const std::string& src)
        : _allocator(allocator), _src(src.c_str(), static_cast<Int>(src.size())), _pos(0){}

    JsonParser::JsonParser(const ::Memory::IAllocator* allocator, DataTypes::StringView&& src)
        : _allocator(allocator), _src(std::move(src)), _pos(0){}

    JsonValue JsonParser::Parse(){
        auto val = this->ParseValue();
        this->SkipWhitespace();
        if (this->_pos != this->_src.Size())
            throw std::runtime_error("JsonParser::Parse: Unexpected trailing characters");
        return val;
    }

    // ── JsonWriter ────────────────────────────────────────────────────────────

    void JsonWriter::PrintIndent(std::ostream& os, const int indent) {
        static constexpr char spaces[128] = "                                                                                                                               ";
        const int n = indent * 4;
        os.write(spaces, n < 128 ? n : 128);
    }

    void JsonWriter::PrintString(std::ostream& os, const JsonString& str) {
        os << JSON_QUOTE;
        for (Int i = 0; i < str.Size(); ++i) {
            const auto character = str[i];
            switch (character) {
                case JSON_QUOTE:     os << "\\\""; break;
                case JSON_BACKSLASH: os << "\\\\"; break;
                case JSON_NEWLINE:   os << "\\n";  break;
                case JSON_CR:        os << "\\r";  break;
                case JSON_TAB:       os << "\\t";  break;
                default:             os << character;      break;
            }
        }
        os << JSON_QUOTE;
    }

    void JsonWriter::PrintValue(std::ostream& os, const JsonValue& value, const int indent) {
        switch (value.Type()) {
            case JsonType::Null:
                os << "null";
                break;
            case JsonType::Bool:
                os << (value.AsBool() ? "true" : "false");
                break;
            case JsonType::Number:
                os << value.AsNumber();
                break;
            case JsonType::String:
                PrintString(os, value.AsString());
                break;
            case JsonType::Array: {
                const auto& arr = value.AsArray();
                if (arr.Size() == 0) { os << "[]"; break; }
                os << JSON_OPEN_BRACKET << JSON_NEWLINE;
                for (Int i = 0; i < arr.Size(); ++i) {
                    PrintIndent(os, indent + 1);
                    PrintValue(os, arr[i], indent + 1);
                    if (i < arr.Size() - 1) os << JSON_COMMA;
                    os << JSON_NEWLINE;
                }
                PrintIndent(os, indent);
                os << JSON_CLOSE_BRACKET;
                break;
            }
            case JsonType::Object: {
                const auto& obj = value.AsObject();
                if (obj.empty()) { os << "{}"; break; }
                os << JSON_OPEN_BRACE << JSON_NEWLINE;
                Int i = 0;
                const auto count = obj.size();
                for (const auto& [key, val] : obj) {
                    PrintIndent(os, indent + 1);
                    PrintString(os, key);
                    os << JSON_COLON << ' ';
                    PrintValue(os, val, indent + 1);
                    if (i < count - 1) os << JSON_COMMA;
                    os << JSON_NEWLINE;
                    ++i;
                }
                PrintIndent(os, indent);
                os << JSON_CLOSE_BRACE;
                break;
            }
        }
    }

    void JsonWriter::Print(std::ostream& os, const JsonValue& value, const int indent) {
        PrintValue(os, value, indent);
        os << JSON_NEWLINE;
    }
}
