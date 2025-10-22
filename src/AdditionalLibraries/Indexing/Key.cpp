#include "Key.h"
#include <cstring>

#include "../DataTypes/Value/Value.h"
#include "../DataTypes/Headers/Headers.h"

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
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
        this->size = 0;
    }

    Key::Key(const void *keyValue, const Constants::key_size_t &keySize, const Constants::DataType& keyType)
    {
        this->value = Value(keyValue, keySize, keyType);
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;

        this->size = keySize;
    }

    Key::Key(const Value &field){
        this->value = field;

        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
        this->size = field.GetSize();
    }

    Key::Key(const vector<Key> &subKeys)
    {
        this->size = 0;
        for (const auto &key : subKeys)
        {
            this->subKeys.push_back(key);
            this->size += key.size;
        }
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey)
    {
        this->size = otherKey.size;

        if(otherKey.subKeys.empty())
        {
            this->value = otherKey.value;
            return;
        }

        this->subKeys = otherKey.subKeys;
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;

        //key is not composite
        // memcpy(this->value, otherKey.value, otherKey.size);

    }

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

    const Value & Key::GetValue() const { return this->value; }

    Key::ComparisonResult Key::CompareCompositeKeys(const Key& otherKey) const
    {
        if(this->indexKeyPosition != -1)
            return Key::CompareSubKeys(this->subKeys[this->currentSearchKeyPosition], otherKey.subKeys[this->indexKeyPosition]);

        for (int i = 0; i < this->subKeys.size(); i++)
        {
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

    int32_t Key::GetIdentityKey()const{
        if (subKeys.empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        return this->subKeys.front().value.GetInt();
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