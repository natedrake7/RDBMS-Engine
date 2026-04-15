#pragma once
#include "DataTypes/Value.h"
#include <vector>
#include "../../CoreEngine/include/DatabaseConstants.h"
#include "DataStructures/PolymorphicArray.h"

namespace DataTypes::Indexing {
  struct Key{

    enum class ComparisonResult : TinyInt {
      Less = -1,
      Equal = 0,
      Greater = 1,
    };

    Value value;
    DataStructures::PolymorphicArray<Key> subKeys;
    key_size_t size;

    explicit Key();
    explicit Key(const ::Memory::IAllocator* allocator);
    Key(
        const void *keyValue,
        key_size_t keySize,
        DataType keyType,
        const Memory::IAllocator* allocator
    );
    explicit Key(const Value& field);
    explicit Key(Value& field);
    explicit Key(const DataStructures::PolymorphicArray<Key>& subKeys);
    explicit Key(DataStructures::PolymorphicArray<Key>& subKeys);
    explicit Key(const Key*& otherKey);
    Key(const Key &otherKey);
    Key(Key&& otherKey) noexcept;
    Key& operator=(Key&& otherKey) noexcept;
    Key& operator=(const Key& otherKey);
    ~Key();

    bool operator==(const Key& otherKey) const;
    bool operator>(const Key& otherKey) const;
    bool operator<(const Key& otherKey) const;
    bool operator<=(const Key& otherKey) const;
    bool operator>=(const Key& otherKey) const;

    [[nodiscard]] bool InClosedRange(const Key& minKey, const Key& maxKey) const;
    [[nodiscard]] bool InOpenRange(const Key& minKey, const Key& maxKey) const;

    [[nodiscard]] bool PartialEqualityCompare(const Key& otherKey) const;
    [[nodiscard]] bool PartialGreaterThan(const Key& otherKey) const;

    [[nodiscard]] const Value &GetValue() const;
    [[nodiscard]] ComparisonResult CompareCompositeKeys(const Key& otherKey) const;
    void InsertKey(const Key &otherKey);
    void InsertKey(Key&& otherKey);

    static ComparisonResult CompareSubKeys(const Key& firstKey, const Key& otherKey);
    [[nodiscard]] Int AsInt(Int pos = 0)const;
    [[nodiscard]] BigInt AsBigInt(Int pos = 0)const;

    [[nodiscard]] key_size_t CalculateSize()const;

    void Serialize(object_t*& buffer, page_offset_t& offset) const;
    static Key DeserializeNonComposite(
        const Memory::IAllocator* allocator,
        const object_t* buffer,
        page_offset_t& offset,
        DataType type
    );
    static Key Deserialize(
        const Memory::IAllocator* allocator,
        const object_t* buffer,
        page_offset_t& offset,
        const UnsignedTinyInt& numberOfSubKeys,
        const std::array<DataType, Constants::MAX_NUMBER_OF_SUB_KEYS>& keyTypes
    );

    DataTypes::String ToString(const Memory::IAllocator* allocator) const;

    friend std::ostream& operator<<(std::ostream& os, const Key& key);
  };

  struct QueryData{
    page_id_t pageId;
    page_offset_t indexPosition;

    QueryData();
    QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition);
    ~QueryData();
  };
}
