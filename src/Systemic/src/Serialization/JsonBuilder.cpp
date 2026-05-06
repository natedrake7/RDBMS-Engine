#include "../include/Serialization/JsonBuilder.h"

namespace Serialization{
    void JsonBuilder::Value(const void* data, const Int size, const JsonType type){
        const auto offSet = this->_buffer.Size();
        this->_buffer.MemoryCopy(data, size);

        auto* parent = this->_containers.Back();

        if (parent->_type == JsonType::Array)
        {
            JsonEntry entry(0, offSet, size);
            parent->_entries.Push(std::move(entry));
        }

        auto* entry = parent->_entries.Back();
        entry->_valueOffset = JsonBuilder::RelativeOffSet(offSet, parent->_headerPosition);
        entry->_valueSize = size;
        entry->_type = static_cast<UnsignedTinyInt>(type);

        if (parent->_type != JsonType::Array)
            parent->_expectingKey = true;
    }

    void JsonBuilder::SetObjectSize(const Int size){
        if (this->_containers.Empty())
            return;

        auto* parent = this->_containers.Back();

        if (parent->_type != JsonType::Object
            && parent->_type != JsonType::Array
        ) return;

        if (parent->_type == JsonType::Object)
            parent->_expectingKey = true;

        auto* entry = parent->_entries.Back();
        //it is the same as entryTable starts where value segment ends
        entry->_valueSize = size;
    }

    JsonBuilder::JsonBuilder(const Memory::IAllocator* allocator)
        : _buffer(allocator), _containers(allocator), _allocator(allocator) {}

    void JsonBuilder::StartObject(){
        if (!this->_containers.Empty()){
            auto* parent = this->_containers.Back();
            if (parent->_type == JsonType::Object
                || parent->_type == JsonType::Array
            ){
                auto* entry = parent->_entries.Back();
                entry->_valueOffset = JsonBuilder::RelativeOffSet(this->_buffer.Size(), parent->_headerPosition);
                entry->_valueSize = 0;
                entry->_type = static_cast<UnsignedTinyInt>(JsonType::Object);
            }
        }

        //even though the container stores the absolute offSet, all offsets written are relative to the parent
        JsonContainer object(this->_allocator, this->_buffer.Size(), JsonType::Object, true);
        this->_containers.Push(std::move(object));

        constexpr JsonHeader header(0, 0, static_cast<UnsignedTinyInt>(JsonType::Object));
        this->_buffer.MemoryCopy(&header, JsonHeader::SIZE);
    }

    void JsonBuilder::EndObject(){
        auto* object = this->_containers.Back();
        this->_containers.Pop();

        auto* header = reinterpret_cast<JsonHeader*>(this->_buffer.Data() + object->_headerPosition);

        header->_entryTablePosition = JsonBuilder::RelativeOffSet(this->_buffer.Size(), object->_headerPosition);
        header->_size = static_cast<UnsignedTinyInt>(object->_entries.Size());

        const auto entryTableSize = object->_entries.Size() * static_cast<Int>(JsonEntry::SIZE);

        this->SetObjectSize(header->_entryTablePosition + entryTableSize);

        std::ranges::sort(object->_entries,
    [](const JsonEntry& a, const JsonEntry& b){
            return a._keyHash < b._keyHash;
        });

        this->_buffer.MemoryCopy(object->_entries.Data(),  entryTableSize);
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

        const auto entryTableSize = object->_entries.Size() * static_cast<Int>(JsonEntry::SIZE);

        this->SetObjectSize(header->_entryTablePosition + entryTableSize);

        this->_buffer.MemoryCopy(object->_entries.Data(),  entryTableSize);

        auto* parent = this->_containers.Back();
        parent->_expectingKey = true;
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

    void JsonBuilder::Value(const DataTypes::String& value){
        this->Value(value.Data(), value.Size(), JsonType::String);
    }

    void JsonBuilder::Value(const bool value){
        this->Value(&value, sizeof(value), JsonType::Bool);
    }

    void JsonBuilder::Value(const DataTypes::Decimal& value){
        this->Value(value.GetRawData(), value.GetRawDataSize(), JsonType::Number);
    }

    void JsonBuilder::ValueNull(){
        const auto offSet = this->_buffer.Size();

        auto* parent = this->_containers.Back();
        auto* entry = parent->_entries.Back();
        entry->_valueOffset = JsonBuilder::RelativeOffSet(offSet, parent->_headerPosition);
        entry->_valueSize = 0;
        entry->_type = static_cast<UnsignedTinyInt>(JsonType::Null);
        if (parent->_type != JsonType::Array)
            parent->_expectingKey = true;
    }

    DataTypes::JsonBinary JsonBuilder::Build(){
        return DataTypes::JsonBinary(this->_allocator, this->_buffer.Data(), this->_buffer.Size());
    }
}
