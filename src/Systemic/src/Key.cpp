#include "../include/Key.h"
#include "../include/DataTypes/Value.h"
#include <stdexcept>
#include <ostream>

#include "Encoding.h"

namespace DataTypes::Indexing{
    Key::Key()
        : _data(nullptr){}

    Key::Key(const object_t* buffer)
        : _data(buffer){}

    Key::Key(
        const ::Memory::IAllocator* allocator,
        const Value& value
    ): _data(nullptr){
        this->InsertSingleKey(allocator, value);
    }

    Key::Key(
        const ::Memory::IAllocator* allocator,
        const DataStructures::PolymorphicArray<Value>& subKeys
    ): _data(nullptr){
        this->InsertKeys(allocator, subKeys);
    }

    Key::Key(Key&& otherKey) noexcept{
        this->_data = otherKey._data;
        otherKey._data = nullptr;
    }

    Key& Key::operator=(Key&& otherKey) noexcept{
        if (this == &otherKey)
            return *this;

        this->_data = otherKey._data;
        otherKey._data = nullptr;

        return *this;
    }

    bool operator==(const Key& lhs, const Key& rhs){
        return Key::Compare(lhs,rhs) == Comparators::Comparator::Equal;
    }

    bool operator>(const Key& lhs, const Key& rhs){
        return Key::Compare(lhs,rhs) == Comparators::Comparator::Greater;
    }

    bool operator<(const Key& lhs, const Key& rhs){
        return Key::Compare(lhs,rhs) == Comparators::Comparator::Less;
    }

    bool operator<=(const Key& lhs, const Key& rhs){
        return Key::Compare(lhs,rhs) != Comparators::Comparator::Greater;
    }

    bool operator>=(const Key& lhs, const Key& rhs){
        return Key::Compare(lhs,rhs) != Comparators::Comparator::Less;
    }

    bool Key::InClosedRange(const Key &minKey, const Key &maxKey) const { return minKey <= *this && maxKey >= *this; }

    bool Key::InOpenRange(const Key &minKey, const Key &maxKey) const { return minKey < *this && maxKey > *this; }

    bool Key::PartialEqualityCompare(const Key& lhs, const Key& rhs){
        const auto compareLength = Math::Min(lhs.Count(), rhs.Count());
        for (Int i = 0; i < compareLength; i++){
            const auto result = CompareEntryAt(lhs, rhs, i);
            if (result != Comparators::Comparator::Equal)
                return false;
        }

        return true;
    }

    bool Key::PartialGreaterThan(const Key& lhs, const Key& rhs){
        const auto compareLength = Math::Min(lhs.Count(), rhs.Count());
        for (Int i = 0; i < compareLength; i++){
            const auto result = CompareEntryAt(lhs, rhs, i);
            if (result == Comparators::Comparator::Greater)
                return false;
        }

        return true;
    }

    Comparators::Comparator Key::Compare(const Key& lhs, const Key& rhs){
        const auto partialCompare = Key::PartialCompare(lhs, rhs);
        if (partialCompare != Comparators::Comparator::Equal)
            return partialCompare;
        return Comparators::Compare(lhs.Count(), rhs.Count());   // shorter prefix sorts first
    }

    Comparators::Comparator Key::PartialCompare(const Key& lhs, const Key& rhs){
        const auto compareLength = Math::Min(lhs.Count(), rhs.Count());
        for (Int i = 0; i < compareLength; i++){
            const auto result = CompareEntryAt(lhs, rhs, i);
            if (result != Comparators::Comparator::Equal)
                return result;
        }
        return Comparators::Comparator::Equal;
    }

    Int Key::AsInt(const Int pos)const{
        if (this->Empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (this->Count() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        const auto* entry = this->GetEntry(pos);
        return Encoding::DecodeInteger<Int>(this->_data + entry->_offset);
    }

    BigInt Key::AsBigInt(const Int pos) const{
        if (this->Empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (this->Count() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        const auto* entry = this->GetEntry(pos);
        return Encoding::DecodeInteger<BigInt>(this->_data + entry->_offset);
    }

    void Key::SetCount(object_t* buffer, const key_size_t count){
        *reinterpret_cast<key_size_t*>(buffer) = count;
    }

    void Key::SetSize(object_t* buffer, const key_size_t size){
        *reinterpret_cast<key_size_t*>(buffer + sizeof(key_size_t)) = size;
    }

    void Key::InsertSingleKey(
        const Memory::IAllocator* allocator,
        const Value& value
    ){
        const auto keySize = value.Size();
        const auto size = Key::HEADER_SIZE + keySize + sizeof(KeyEntry);

        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(size));

        auto* entry = reinterpret_cast<KeyEntry*>(buffer + Key::HEADER_SIZE);
        entry->_offset = Key::HEADER_SIZE + sizeof(KeyEntry);
        entry->_meta = KeyEntry::EncodeMeta(
            value.IsNull(),
            Key::EncodeValue(buffer + Key::HEADER_SIZE + sizeof(KeyEntry), value)
        );
        Key::SetCount(buffer, 1);
        Key::SetSize(buffer, Key::HEADER_SIZE + sizeof(KeyEntry) + keySize);

        this->_data = buffer;
    }

    void Key::InsertKeys(
        const Memory::IAllocator* allocator,
        const DataStructures::PolymorphicArray<Value>& subKeys
    ){
        auto size = Key::HEADER_SIZE;
        for (const auto& value : subKeys)
            size += static_cast<key_size_t>(value.Size() + sizeof(KeyEntry));

        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(size));
        auto dataOffset = Key::HEADER_SIZE + subKeys.Size() * sizeof(KeyEntry);
        for (Int i = 0; i < subKeys.Size(); i++){
            auto* entry = reinterpret_cast<KeyEntry*>(buffer + Key::HEADER_SIZE + i * sizeof(KeyEntry));
            entry->_offset = dataOffset;
            const auto sizeWritten = Key::EncodeValue(buffer + dataOffset, subKeys[i]);
            dataOffset += sizeWritten;
            entry->_meta = KeyEntry::EncodeMeta(subKeys[i].IsNull(), sizeWritten);
        }
        Key::SetCount(buffer, subKeys.Size());
        Key::SetSize(buffer, size);
        this->_data = buffer;
    }

    key_size_t Key::EncodeValue(object_t* buffer, const Value& value){
        if (value.IsNull())
            return 0;

        switch (value.GetType()){
        case DataType::TinyInt:
            return Encoding::EncodeInteger<TinyInt>(buffer, value.AsTinyInt());
        case DataType::SmallInt:
            return Encoding::EncodeInteger<SmallInt>(buffer, value.AsSmallInt());
        case DataType::Int:
            return Encoding::EncodeInteger<Int>(buffer, value.AsInt());
        case DataType::BigInt:
        case DataType::DateTime:
            return Encoding::EncodeInteger<BigInt>(buffer, value.AsBigInt());
        case DataType::Bool:
        case DataType::String:
        case DataType::Decimal:
        case DataType::Guid:{
            std::memcpy(buffer, value.Data(), value.Size());
            return value.Size();
        }
        default:
            throw std::runtime_error("Key::EncodeValue: invalid value type");
        }
    }

    bool Key::Empty() const{
        return this->Count() == 0;
    }

    Comparators::Comparator Key::CompareEntryAt(const Key& lhs, const Key& rhs, const Int index){
        const auto* lhsEntry = lhs.GetEntry(index);
        const auto* rhsEntry = rhs.GetEntry(index);

        if (lhsEntry->IsNull() || rhsEntry->IsNull())
            return Comparators::Compare(lhsEntry->IsNull(), rhsEntry->IsNull());

        const auto minSize = Math::Min(lhsEntry->Size(), rhsEntry->Size());
        const auto cmp = std::memcmp(lhs._data + lhsEntry->_offset, rhs._data + rhsEntry->_offset, minSize);
        if (cmp != 0)
            return Comparators::Compare(cmp, 0);

        return Comparators::Compare(lhsEntry->Size(), rhsEntry->Size());
    }

    key_size_t Key::Count() const{
        return *reinterpret_cast<const key_size_t*>(this->_data);
    }

    const KeyEntry* Key::GetEntry(const Int index) const{
        return reinterpret_cast<const KeyEntry*>(this->_data + HEADER_SIZE + index * sizeof(KeyEntry));
    }

    key_size_t Key::Size() const{
        return *reinterpret_cast<const key_size_t*>(this->_data + sizeof(key_size_t));
    }

    String Key::ToString(const Memory::IAllocator* allocator) const{
        auto str = String::Empty(allocator);
        // if (!this->subKeys.Empty()){
        //     str.Append("(");
        //
        //     for (int i = 0; i < this->subKeys.Size(); i++) {
        //         const auto& subKey = this->subKeys[i];
        //
        //         str.Append(subKey.ToString(allocator));
        //         if (i != this->subKeys.Size() - 1)
        //             str.Append(", ");
        //     }
        //
        //     str.Append(")");
        // }
        //
        // if (this->value.IsNull())
        //     return str;
        //
        // str.Append(this->value.AsString());
        return str;
    }

    std::ostream & operator<<(std::ostream &os, const Key &key){
        // if(!key.subKeys.Empty()){
        //     os << "(";
        //
        //     for (int i = 0; i < key.subKeys.Size(); i++) {
        //         const auto& subKey = key.subKeys[i];
        //
        //         os << subKey;
        //
        //         if (i != key.subKeys.Size() - 1)
        //             os << ", ";
        //     }
        //
        //     os << ")";
        //
        //     return os;
        // }
        //
        // os << key.value;
        return os;
    }

}
