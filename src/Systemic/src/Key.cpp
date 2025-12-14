#include "../include/Key.h"
#include "../include/DataTypes/Value.h"

namespace DataTypes::Indexing{
   QueryData::QueryData()
    {
        this->indexPosition = 0;
        this->pageId = 0;
    }

   QueryData::QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition)
    {
        this->pageId = pageId;
        this->indexPosition = otherIndexPosition;
    }

   QueryData::~QueryData() = default;

    Key::Key()
    {
        this->value = Value::Null();
        this->size = 0;
    }

    Key::Key(const void *keyValue, const key_size_t &keySize, const DataType& keyType)
    {
        this->value = Value(keyValue, keySize, keyType);
        this->size = keySize;
    }

    Key::Key(const Value &field){
        this->value = field;
        this->size = this->value.GetSize();
    }

    Key::Key(Value &field) {
        this->value = std::move(field);
        this->size = this->value.GetSize();
    }

    Key::Key(const vector<Key> &subKeys)
    {
        this->size = 0;
        for (const auto &key : subKeys){
            this->subKeys.push_back(key);
            this->size += key.size;
        }
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey)
    {
        this->size = otherKey.size;

        if(otherKey.subKeys.empty()){
            this->value = otherKey.value;
            return;
        }

        this->subKeys = otherKey.subKeys;
    }

    Key::Key(const Key *&otherKey) {
        this->size = otherKey->size;

        if(otherKey->subKeys.empty()){
            this->value = otherKey->value;
            return;
        }

        this->subKeys = otherKey->subKeys;
    }

    // Key::Key(Key &&other) noexcept {
    //     if (this == &other)
    //         return;
    //
    //     this->size = other.size;
    //
    //     if(other.subKeys.empty()){
    //         this->value = other.value;
    //         other.value = Value(nullptr, 0);
    //         return;
    //     }
    //
    //     this->subKeys = other.subKeys;
    //     this->indexKeyPosition = -1;
    //     this->currentSearchKeyPosition = -1;
    //
    //     other.size = 0;
    //     other.subKeys.clear();
    // }

    void Key::InsertKey(const Key &otherKey)
    {
        this->size += (otherKey.size + sizeof(key_size_t));

        this->subKeys.push_back(otherKey);
    }

    bool Key::operator>(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) > Key::ComparisonResult::Equal;

        const auto result = this->value > otherKey.value;

        return result.GetBool();
    }

    bool Key::operator<(const Key& otherKey) const
    {
        return !(*this >= otherKey);
    }

    bool Key::operator<=(const Key& otherKey) const
    {
        return !(*this > otherKey);
    }

    bool Key::operator>=(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) >= Key::ComparisonResult::Equal;

        const auto result = this->value >= otherKey.value;

        return result.GetBool();
    }

    bool Key::InClosedRange(const Key &minKey, const Key &maxKey) const { return minKey <= *this && maxKey >= *this; }

    bool Key::InOpenRange(const Key &minKey, const Key &maxKey) const { return minKey < *this && maxKey > *this; }

    bool Key::PartialEqualityCompare(const Key &otherKey) const {
        const auto compareLength = std::min(this->subKeys.size(), otherKey.subKeys.size());

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
        const auto compareLength = std::min(this->subKeys.size(), otherKey.subKeys.size());

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
        for (int i = 0; i < this->subKeys.size(); i++){
            if (this->subKeys[i] == otherKey.subKeys[i])
                continue;

            if (this->subKeys[i] < otherKey.subKeys[i])
                return ComparisonResult::Less;

            return ComparisonResult::Greater;
        }

        return ComparisonResult::Equal;
    }

    Key::ComparisonResult Key::CompareSubKeys(const Key& firstKey, const Key& otherKey)
    {
        if (firstKey == otherKey)
            return ComparisonResult::Equal;

        if (firstKey < otherKey)
            return ComparisonResult::Less;

        return ComparisonResult::Greater;
    }

    int32_t Key::AsInt(const int& pos)const{
        if (subKeys.empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys.at(pos).value.GetInt();
    }

    int64_t Key::AsBigInt(const int& pos) const{
        if (subKeys.empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys.at(pos).value.GetBigInt();
    }

    std::ostream & operator<<(std::ostream &os, const Key &key){
        if(!key.subKeys.empty())
        {
            os << "(";

            for (int i = 0; i < key.subKeys.size(); i++) {
                const auto& subKey = key.subKeys[i];

                os << subKey;

                if (i != key.subKeys.size() - 1)
                    os << ", ";
            }

            os << ")";

            return os;
        }

        os << key.value;
        return os;
    }

    bool Key::operator==(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) == Key::ComparisonResult::Equal;

        const auto result = this->value == otherKey.value;

        return result.GetBool();
    }

}