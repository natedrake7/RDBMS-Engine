#pragma once
#include <vector>
#include "../../DatabaseEngine/include/Constants.h"
#include "DataTypes/Value.h"

namespace DataTypes::Indexing {
  struct Key
  {
    enum class ComparisonResult : int8_t {
      Less = -1,
      Equal = 0,
      Greater = 1,
    };

    key_size_t size;

    Value value;
    std::vector<Key> subKeys;


    Key();
    Key(const void *keyValue, const key_size_t &keySize, const DataType& keyType);
    explicit Key(const Value& field);
    explicit Key(Value& field);
    explicit Key(const std::vector<Key>& subKeys);
    explicit Key(const Key*& otherKey);
    Key(const Key &otherKey);
    ~Key();

    // Key(Key&& other)noexcept;

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

    static ComparisonResult CompareSubKeys(const Key& firstKey, const Key& otherKey);
    [[nodiscard]] int32_t AsInt(const int& pos = 0)const;
    [[nodiscard]] int64_t AsBigInt(const int& pos = 0)const;

    friend std::ostream& operator<<(std::ostream& os, const Key& key);
  };

  struct QueryData
  {
    page_id_t pageId;
    page_offset_t indexPosition;

    QueryData();
    QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition);
    ~QueryData();
  };
}