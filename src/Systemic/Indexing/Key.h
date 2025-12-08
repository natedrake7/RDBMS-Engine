#pragma once
#include <vector>
#include "../../Database/Constants.h"
#include  "../DataTypes/Value/Value.h"

namespace DataTypes::Indexing {
  struct Key
  {
    enum class ComparisonResult : int8_t {
      Less = -1,
      Equal = 0,
      Greater = 1,
    };

    Constants::key_size_t size;

    Value value;
    vector<Key> subKeys;


    Key();
    Key(const void *keyValue, const Constants::key_size_t &keySize, const Constants::DataType& keyType);
    explicit Key(const Value& field);
    explicit Key(Value& field);

    explicit Key(const std::vector<Key>& subKeys);
    ~Key();

    Key(const Key &otherKey);

    bool operator==(const Key& otherKey) const;
    bool operator>(const Key& otherKey) const;
    bool operator<(const Key& otherKey) const;
    bool operator<=(const Key& otherKey) const;
    bool operator>=(const Key& otherKey) const;
    bool InClosedRange(const Key& minKey, const Key& maxKey) const;
    bool InOpenRange(const Key& minKey, const Key& maxKey) const;

    [[nodiscard]] const Value &GetValue() const;
    [[nodiscard]] ComparisonResult CompareCompositeKeys(const Key& otherKey) const;
    void InsertKey(const Key &otherKey);

    static ComparisonResult CompareSubKeys(const Key& firstKey, const Key& otherKey);
    [[nodiscard]] int32_t GetKeyAsInt()const;
    [[nodiscard]] int64_t GetKeyAsBigInt()const;

    //key comparison index used only on queries and not on key saveon db
    int indexKeyPosition = -1;
    int currentSearchKeyPosition = -1;

    friend std::ostream& operator<<(std::ostream& os, const Key& key);
  };

  struct QueryData
  {
    Constants::page_id_t pageId;
    Constants::page_offset_t indexPosition;

    QueryData();
    QueryData(const Constants::page_id_t &pageId, const Constants::page_offset_t &otherIndexPosition);
    ~QueryData();
  };
}