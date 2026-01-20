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

    Key::Key(){
        this->value = Value::Null();
        this->size = 0;
    }

    Key::Key(const void *keyValue, const key_size_t keySize, const DataType keyType){
        this->value = Value(keyValue, keySize, keyType);
        this->size = keySize;
    }

    Key::Key(const Value &field){
        this->value = field;
        this->size = this->value.Size();
    }

    Key::Key(Value &field) {
        this->value = std::move(field);
        this->size = this->value.Size();
    }

    Key::Key(const std::vector<Key> &subKeys){
        this->size = 0;
        for (const auto &key : subKeys){
            this->subKeys.push_back(key);
            this->size += key.size;
        }
    }

    Key::Key(std::vector<Key>& subKeys){
        this->subKeys = std::move(subKeys);
        this->size = this->CalculateSize();
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey){
        this->size = otherKey.size;

        if(otherKey.subKeys.empty()){
            this->value = otherKey.value;
            return;
        }

        this->subKeys = otherKey.subKeys;
    }

    Key::Key(Key&& otherKey) noexcept{
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

    bool Key::operator==(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) == Key::ComparisonResult::Equal;

        const auto result = this->value == otherKey.value;

        return result.AsBool();
    }

    bool Key::operator>(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) > Key::ComparisonResult::Equal;

        const auto result = this->value > otherKey.value;

        return result.AsBool();
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

        return result.AsBool();
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

    void Key::InsertKey(const Key &otherKey)
    {
        this->size += (otherKey.size + sizeof(key_size_t));

        this->subKeys.push_back(otherKey);
    }

    Key::ComparisonResult Key::CompareSubKeys(const Key& firstKey, const Key& otherKey){
        if (firstKey == otherKey)
            return ComparisonResult::Equal;

        if (firstKey < otherKey)
            return ComparisonResult::Less;

        return ComparisonResult::Greater;
    }

    Int Key::AsInt(const Int pos)const{
        if (subKeys.empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys.at(pos).value.AsInt();
    }

    BigInt Key::AsBigInt(const Int pos) const{
        if (subKeys.empty())
            throw std::runtime_error("Key::GetIdentityKey: subKeys is empty");

        if (subKeys.size() < pos)
            throw std::runtime_error("Key::GetIdentityKey: invalid key position specified");

        return this->subKeys.at(pos).value.AsBigInt();
    }

    key_size_t Key::CalculateSize()const{
        if (this->subKeys.empty())
            return this->value.Size() + sizeof(key_size_t);

        key_size_t currentSize = 0;
        for (const auto& key : this->subKeys)
            currentSize += key.CalculateSize();

        return currentSize;
    }

    void Key::Serialize(object_t*& buffer, page_offset_t& offset) const{
        if (subKeys.empty()){
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
        const object_t* buffer,
        page_offset_t& offset,
        const DataType type
    ) {
        key_size_t valueSize = 0;
        memcpy(&valueSize, buffer + offset, sizeof(key_size_t));
        offset += sizeof(key_size_t);

        auto* valueData = std::malloc(valueSize);
        memcpy(valueData, buffer + offset, valueSize);
        offset += valueSize;

        Value value(valueData, valueSize, type);
        std::free(valueData);

        return Key(value);
    }

    Key Key::Deserialize(
        const object_t* buffer,
        page_offset_t& offset,
        const UnsignedTinyInt& numberOfSubKeys,
        const std::array<DataType, MAX_NUMBER_OF_SUB_KEYS>& keyTypes
    ){
        std::vector<Key> subKeys;
        subKeys.reserve(numberOfSubKeys);
        for (key_size_t i = 0; i < numberOfSubKeys; i++){
            auto subKey = Key::DeserializeNonComposite(buffer, offset, keyTypes[i]);
            subKeys.push_back(std::move(subKey));
        }

        return Key(subKeys);
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

}