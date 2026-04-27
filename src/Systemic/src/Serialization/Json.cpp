#include <utility>

#include "../include/Serialization/Json.h"

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

    void JsonParser::SkipWhitespace(){
        while (this->_pos < this->_src.Size() && std::isspace(this->_src[this->_pos]))
            this->_pos++;
    }

    void JsonParser::SkipComment(){
        if (this->_src.Size() < this->_pos + 2)
            return;

        if (this->_src[this->_pos] == '/' && this->_src[this->_pos + 1] == '/') {
            while (this->_pos < this->_src.Size() && this->_src[this->_pos] != '\n')
                this->_pos++;
        }
    }

    char JsonParser::Peek() const{
        return this->_pos < this->_src.Size() ? this->_src[this->_pos] : '\0';
    }

    char JsonParser::Consume(){
        return this->_src[this->_pos++];
    }

    JsonValue JsonParser::ParseValue(){
        this->SkipWhitespace();
        this->SkipComment();

        const auto character = this->Peek();
        if (character == '"')
            return JsonValue(this->ParseString());
        if (character == '{')
            return JsonValue(this->ParseObject());
        if (character == '[')
            return JsonValue(this->ParseArray());
        if (character == 't' || character == 'f')
            return JsonValue(this->ParseBool());
        if (character == 'n')
            return JsonValue(this->ParseNull());
        if (character == '-' || std::isdigit(character))
            return JsonValue(this->ParseNumber());
        throw std::runtime_error(std::string("Unexpected character: ") + character);
    }

    JsonString JsonParser::ParseString(){
        this->Consume();

        DataTypes::String result(this->_allocator);
        while (this->_pos < this->_src.Size() && this->Peek() != '"'){
            if (this->Peek() != '\\'){
                result.Append(this->Consume());
                continue;
            }

            this->Consume();
            switch (this->Consume()) {
                case '"':  result.Append('"');  break;
                case '\\': result.Append('\\'); break;
                case '/':  result.Append('/');  break;
                case 'n':  result.Append('\n'); break;
                case 't':  result.Append('\t'); break;
                case 'r':  result.Append('\r'); break;
                default: throw std::runtime_error("JsonParser::ParseString: Invalid escape sequence. Error at position: " + std::to_string(this->_pos));
            }
        }

        if (this->Peek() != '"')
            throw std::runtime_error("JsonParser::ParseString: Unterminated string. Error at position: " + std::to_string(this->_pos));
        this->Consume();

        return result;
    }

    JsonNumber JsonParser::ParseNumber() {
        const auto start = this->_pos;

        if (this->Peek() == '-')
            this->Consume();

        while (std::isdigit(this->Peek()))
            this->Consume();

        // Decimal part
        if (this->Peek() == '.') {
            this->Consume();
            while (std::isdigit(this->Peek()))
                this->Consume();
        }

        // Exponent part
        if (this->Peek() == 'e' || this->Peek() == 'E') {
            this->Consume();
            if (this->Peek() == '+' || this->Peek() == '-')
                this->Consume();
            while (std::isdigit(this->Peek()))
                this->Consume();
        }

        const auto view = DataTypes::StringView(this->_src.Data() + start, this->_pos - start);
        return JsonNumber(view);
    }

    JsonBool JsonParser::ParseBool(){
        if (DataTypes::String::SubString(this->_src, this->_pos, this->_pos + 4) == "true"){
            this->_pos += 4;
            return true;
        }

        if (DataTypes::String::SubString(this->_src, this->_pos, this->_pos + 5) == "false"){
            this->_pos += 5;
            return false;
        }

        throw std::runtime_error("JsonParser::ParseBool: Invalid boolean value. Error at position: " + std::to_string(this->_pos));
    }

    JsonNull JsonParser::ParseNull(){
        if (DataTypes::String::SubString(this->_src, this->_pos, this->_pos + 4) == "null"){
            this->_pos += 4;
            return nullptr;
        }

        throw std::runtime_error("JsonParser::ParseNull: Invalid null value. Error at position: " + std::to_string(this->_pos));
    }

    JsonArray JsonParser::ParseArray(){
        this->Consume();

        JsonArray array(this->_allocator);
        this->SkipWhitespace();
        if (this->Peek() == ']') {
            this->Consume();
            return array;
        }

        while (true) {
            array.Push(this->ParseValue());
            this->SkipWhitespace();
            if (this->Peek() == ']') {
                this->Consume();
                break;
            }
            if (this->Peek() != ',')
                throw std::runtime_error("JsonParser::ParseArray: Expected ',' in array. Error at position: " + std::to_string(this->_pos));
            this->Consume();
        }

        return array;
    }

    JsonObject JsonParser::ParseObject(){
        this->Consume();

        JsonObject object;
        this->SkipWhitespace();
        if (this->Peek() == '}')
        {
            this->Consume();
            return object;
        }
        while (true) {
            this->SkipWhitespace();

            if (this->Peek() != '"') {
                throw std::runtime_error("JsonParser::ParseObject: Expected string key. Error at position: " + std::to_string(this->_pos));
            }

            auto key = this->ParseString();
            this->SkipWhitespace();
            if (this->Consume() != ':') {
                throw std::runtime_error("JsonParser::ParseObject: Expected ':' in object. Error at position: " + std::to_string(this->_pos));
            }
            object[key] = this->ParseValue();
            this->SkipWhitespace();
            if (this->Peek() == '}'){
                this->Consume();
                break;
            }

            if (this->Peek() != ','){
                throw std::runtime_error("JsonParser::ParseObject: Expected ',' in object. Error at position: " + std::to_string(this->_pos));
            }

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
}
