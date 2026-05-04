#include "../include/Serialization/JsonBuilder.h"

namespace Serialization{
    JsonBuilder::JsonBuilder(const Memory::IAllocator* allocator)
        : _buffer(allocator), _containers(allocator), _allocator(allocator) {}

    void JsonBuilder::StartObject(){
        auto offSet = 0;
        if (!this->_containers.Empty()){
            auto* parent = this->_containers.Back();
            if (parent->_type == JsonType::Object
                || parent->_type == JsonType::Array
            ){
                offSet = JsonBuilder::RelativeOffSet(this->_buffer.Size(), parent->_headerPosition);
                auto* entry = parent->_entries.Back();
                entry->_valueOffset = offSet;
                entry->_valueSize = 0;
                entry->_type = static_cast<UnsignedTinyInt>(JsonType::Object);
            }
        }

        JsonContainer object(this->_allocator, offSet, JsonType::Object, true);
        this->_containers.Push(std::move(object));

        constexpr JsonHeader header(0, 0, static_cast<UnsignedTinyInt>(JsonType::Object));
        this->_buffer.MemoryCopy(&header, JsonHeader::SIZE);
    }

    void JsonBuilder::EndObject(){
        auto* object = this->_containers.Back();
        this->_containers.Pop();

        if (!this->_containers.Empty()){
            auto* parent = this->_containers.Back();
            if (parent->_type == JsonType::Object
                || parent->_type == JsonType::Array
            ){
                auto* entry = parent->_entries.Back();
                entry->_valueSize = object->_entries.Size();
            }
        }

        auto* header = reinterpret_cast<JsonHeader*>(this->_buffer.Data() + object->_headerPosition);

        header->_entryTablePosition = JsonBuilder::RelativeOffSet(this->_buffer.Size(), object->_headerPosition);
        header->_size = static_cast<UnsignedTinyInt>(object->_entries.Size());

        std::ranges::sort(object->_entries,
    [](const JsonEntry& a, const JsonEntry& b){
            return a._keyHash < b._keyHash;
        });

        this->_buffer.MemoryCopy(object->_entries.Data(),  object->_entries.Size() * static_cast<Int>(JsonEntry::SIZE));
    }

    void JsonBuilder::StartArray(){
        auto offSet = this->_buffer.Size();
        if (!this->_containers.Empty()){
            auto* parent = this->_containers.Back();
            if (parent->_type == JsonType::Object
                || parent->_type == JsonType::Array
            ){
                offSet = JsonBuilder::RelativeOffSet(offSet, parent->_headerPosition);
            }
        }

        JsonContainer object(this->_allocator, offSet, JsonType::Array, true);
        this->_containers.Push(std::move(object));

        constexpr JsonHeader header(0, static_cast<UnsignedTinyInt>(JsonType::Array));
        this->_buffer.MemoryCopy(&header, JsonHeader::SIZE);
    }

    void JsonBuilder::EndArray(){
        auto* object = this->_containers.Back();
        this->_containers.Pop();

        auto* header = reinterpret_cast<JsonHeader*>(this->_buffer.Data() + object->_headerPosition);

        header->_entryTablePosition = JsonBuilder::RelativeOffSet(this->_buffer.Size(), object->_headerPosition);
        header->_size = static_cast<UnsignedTinyInt>(object->_entries.Size());

        this->_buffer.MemoryCopy(object->_entries.Data(),  object->_entries.Size() * static_cast<Int>(JsonEntry::SIZE));
    }

    void JsonBuilder::Key(const DataTypes::StringView& key){
        auto* object = this->_containers.Back();

        if (!object->_expectingKey)
            throw std::runtime_error("JsonBuilder::Key: Expected value but key was provided");

        const auto offSet = JsonBuilder::RelativeOffSet(this->_buffer.Size(), object->_headerPosition);

        //value offSet will be set afterward
        const auto size = key.Size();
        const auto hash = std::hash<DataTypes::StringView>{}(key);

        JsonEntry entry(hash, offSet, size);
        object->_entries.Push(std::move(entry));

        this->_buffer.MemoryCopy(key.Data(), size);
        object->_expectingKey = false;
    }

    void JsonBuilder::Value(const JsonValue& value){
        const auto offSet = this->_buffer.Size();

        const auto type = value.Type();
        const auto size = value.Size();
        this->_buffer.MemoryCopy(value.Data(), size);

        auto* parent = this->_containers.Back();
        auto* entry = parent->_entries.Back();
        entry->_valueOffset = JsonBuilder::RelativeOffSet(offSet, parent->_headerPosition);
        entry->_valueSize = size;
        entry->_type = static_cast<UnsignedTinyInt>(type);
        if (parent->_type != JsonType::Array)
            parent->_expectingKey = true;
    }

    DataTypes::JsonBinary JsonBuilder::Build(){
        return DataTypes::JsonBinary(this->_allocator, this->_buffer.Data(), this->_buffer.Size());
    }
}
