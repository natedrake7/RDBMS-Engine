#include "../include/Key.h"
#include "../include/DataTypes/Value.h"
#include <array>
#include <stdexcept>
#include <ostream>

namespace DataTypes::Indexing{
    Key::Key()
        : value(Value::Null(nullptr)), size(0){}

    Key::Key(const Memory::IAllocator* allocator)
        : value(Value::Null(nullptr)), subKeys(allocator), size(0){}

    Key::Key(
        const void *keyValue,
        const key_size_t keySize,
        const DataType keyType,
        const Memory::IAllocator* allocator
    ) : value(static_cast<const object_t*>(keyValue), keySize, keyType, allocator), subKeys(allocator), size(keySize){}

    Key::Key(const Value &field)
        : value(nullptr){
        this->value = field;
        this->size = this->value.Size();
    }

    Key::Key(Value &field)
        : value(nullptr){
        this->value = std::move(field);
        this->size = this->value.Size();
    }

    Key::Key(const DataStructures::PolymorphicArray<Key>& subKeys)
        : value(nullptr){
        this->size = 0;

        this->subKeys.TrySetAllocator(subKeys.GetAllocator());
        for (const auto &key : subKeys){
            this->subKeys.Push(key);
            this->size += key.size;
        }
    }

    Key::Key(DataStructures::PolymorphicArray<Key>& subKeys)
        : value(nullptr){
        this->subKeys = std::move(subKeys);
        this->size = this->CalculateSize();
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey)
        : value(nullptr){
        this->size = otherKey.size;

        if(otherKey.subKeys.Empty()){
            this->value = otherKey.value;
            return;
        }

        this->subKeys = otherKey.subKeys;
    }

    Key::Key(Key&& otherKey) noexcept
        : value(nullptr){
        if (this == &otherKey)
            return;

        this->size = otherKey.size;
        this->subKeys = std::move(otherKey.subKeys);
        this->value = std::move(otherKey.value);
    }

    Key& Key::operator=(Key&& otherKey) noexcept{
        if (this == &otherKey)
            return *this;

        this->size = otherKey.size;
        this->subKeys = std::move(otherKey.subKeys);
        this->value = std::move(otherKey.value);

        return *this;
    }

    Key& Key::operator=(const Key& otherKey){
        if (this == &otherKey)
            return *this;

        this->size = otherKey.size;
        this->subKeys = otherKey.subKeys;
        this->value = otherKey.value;

        return *this;
    }

    Key::Key(const Key *&otherKey)
        : value(nullptr){
        this->size = otherKey->size;

        if(otherKey->subKeys.Empty()){
            this->value = otherKey->value;
            return;
        }

        this->subKeys = otherKey->subKeys;
    }

    bool Key::operator==(const Key& otherKey) const{
        if(!this->subKeys.Empty())
            return this->CompareCompositeKeys(otherKey) == Key::ComparisonResult::Equal;

        return this->value == otherKey.value;
    }

    bool Key::operator>(const Key& otherKey) const{
        if(!this->subKeys.Empty())
            return this->CompareCompositeKeys(otherKey) > Key::ComparisonResult::Equal;

        return this->value > otherKey.value;
    }

    bool Key::operator<(const Key& otherKey) const{
        return !(*this >= otherKey);
    }

    bool Key::operator<=(const Key& otherKey) const{
        return !(*this > otherKey);
    }

    bool Key::operator>=(const Key& otherKey) const{
        if(!this->subKeys.Empty())
            return this->CompareCompositeKeys(otherKey) >= Key::ComparisonResult::Equal;

        return this->value >= otherKey.value;
    }

    bool Key::InClosedRange(const Key &minKey, const Key &maxKey) const { return minKey <= *this && maxKey >= *this; }

    bool Key::InOpenRange(const Key &minKey, const Key &maxKey) const { return minKey < *this && maxKey > *this; }

    bool Key::PartialEqualityCompare(const Key &otherKey) const {
        const auto compareLength = std::min(this->subKeys.Size(), otherKey.subKeys.Size());

        for (int i = 0; i < compareLength; i++){
            if (this->subKeys[i] == otherKey.subKeys[i])
                continue;

            if (this->subKeys[i] < otherKey.subKeys[i])
                return false;

            return false;
        }

        return true;
    }

    bool Key::PartialGreaterThan(const Key &otherKey) const {
        const auto compareLength = std::min(this->subKeys.Size(), otherKey.subKeys.Size());

        for (int i = 0; i < compareLength; i++){
            if (this->subKeys[i] == otherKey.subKeys[i])
                continue;

            if (this->subKeys[i] < otherKey.subKeys[i])
                return false;

            return true;
        }

        return true;
    }

    const Value & Key::GetValue() const { return this->value; }

    Key::ComparisonResult Key::CompareCompositeKeys(const Key& otherKey) const{
        const auto numOfKeys = std::min(this->subKeys.Size(), otherKey.subKeys.Size());

        for (int i = 0; i < numOfKeys; i++){
            if (this->subKeys[i] == otherKey.subKeys[i])
                continue;

            if (this->subKeys[i] < otherKey.subKeys[i])
                return ComparisonResult::Less;

            return ComparisonResult::Greater;
        }

        return ComparisonResult::Equal;
    }

    void Key::InsertKey(const Key &otherKey){
        this->size += (otherKey.size + sizeof(key_size_t));
        this->subKeys.Push(otherKey);
    }

    void Key::InsertKey(Key&& otherKey){
        this->size += (otherKey.size + sizeof(key_size_t));
        this->subKeys.Push(std::move(otherKey));
    }

    Key::ComparisonResult Key::CompareSubKeys(const Key& firstKey, const Key& otherKey){
        if (firstKey == otherKey)
            return ComparisonResult::Equal;

        if (firstKey < otherKey)
            return ComparisonResult::Less;

        return ComparisonResult::Greater;
    }

    Int Key::AsInt(const Int pos)const{
        if (this->subKeys.Empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.Size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys[pos].value.AsInt();
    }

    BigInt Key::AsBigInt(const Int pos) const{
        if (this->subKeys.Empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.Size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys[pos].value.AsBigInt();
    }

    key_size_t Key::CalculateSize()const{
        if (this->subKeys.Empty())
            return this->value.Size() + sizeof(key_size_t);

        key_size_t currentSize = 0;
        for (const auto& key : this->subKeys)
            currentSize += key.CalculateSize();

        return currentSize;
    }

    void Key::Serialize(object_t*& buffer, page_offset_t& offset) const{
        if (this->subKeys.Empty()){
            const auto valueSize = this->value.Size();
            memcpy(buffer + offset, &valueSize, sizeof(key_size_t));
            offset += sizeof(key_size_t);

            memcpy(buffer + offset, this->value.Data(), valueSize);
            offset += valueSize;
            return;
        }

        for (const auto& key : this->subKeys)
            key.Serialize(buffer, offset);
    }

    Key Key::DeserializeNonComposite(
        const Memory::IAllocator* allocator,
        const object_t* buffer,
        page_offset_t& offset,
        const DataType type
    ) {
        key_size_t valueSize = 0;
        std::memcpy(&valueSize, buffer + offset, sizeof(key_size_t));
        offset += sizeof(key_size_t);

        auto* data = static_cast<object_t*>(allocator->AllocateRaw(valueSize));

        std::memcpy(data, buffer + offset, valueSize);
        offset += valueSize;

        auto value = Value::FromMove(
            data,
            valueSize,
            type,
            allocator
        );

        return Key(value);
    }

    Key Key::Deserialize(
        const Memory::IAllocator* allocator,
        const object_t* buffer,
        page_offset_t& offset,
        const UnsignedTinyInt& numberOfSubKeys,
        const DataType* keyTypes
    ){
        DataStructures::PolymorphicArray<Key> subKeys(allocator);
        subKeys.Reserve(numberOfSubKeys);
        for (key_size_t i = 0; i < numberOfSubKeys; i++){
            auto subKey = Key::DeserializeNonComposite(allocator, buffer, offset, keyTypes[i]);
            subKeys.Push(std::move(subKey));
        }

        return Key(subKeys);
    }

    String Key::ToString(const Memory::IAllocator* allocator) const{
        auto str = String::Empty(allocator);
        if (!this->subKeys.Empty()){
            str.Append("(");

            for (int i = 0; i < this->subKeys.Size(); i++) {
                const auto& subKey = this->subKeys[i];

                str.Append(subKey.ToString(allocator));
                if (i != this->subKeys.Size() - 1)
                    str.Append(", ");
            }

            str.Append(")");
        }

        if (this->value.IsNull())
            return str;

        str.Append(this->value.AsString());
        return str;
    }

    std::ostream & operator<<(std::ostream &os, const Key &key){
        if(!key.subKeys.Empty()){
            os << "(";

            for (int i = 0; i < key.subKeys.Size(); i++) {
                const auto& subKey = key.subKeys[i];

                os << subKey;

                if (i != key.subKeys.Size() - 1)
                    os << ", ";
            }

            os << ")";

            return os;
        }

        os << key.value;
        return os;
    }

}