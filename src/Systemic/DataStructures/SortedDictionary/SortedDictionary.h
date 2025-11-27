#pragma once
#include <map>

template<typename Key, typename Value>
class SortedDictionary : public std::map<Key, Value>
{
public:
  SortedDictionary() : std::map<Key, Value>() {}

  SortedDictionary(const std::initializer_list<std::pair<const Key, Value>>& values) : std::map<Key, Value>(values) {}

  bool TryGetValue(const Key& key, Value& value) const
  {
    auto it = this->find(key);
    if (it != this->end())
    {
      value = it->second;
      return true;
    }

    return false;
  }

  bool Contains(const Key& key) const
  {
    return this->find(key) != this->end();
  }

  void Remove(const Key& key)
  {
    this->erase(key);
  }

  void Add(const Key& key, const Value& value)
  {
    this->insert(std::make_pair(key, value));
  }

  void Update(const Key& key, const Value& value)
  {
    if (!this->Contains(key))
      return;

    this->at(key) = value;
  }

  Value& Get(const Key& key)
  {
    return this->at(key);
  }

  Value Get(const Key& key) const
  {
    return this->at(key);
  }

  Value FirstOrDefault() const
  {
    if (this->empty())
      return Value();

    return this->begin()->second;
  }

  Value LastOrDefault() const
  {
    if (this->empty())
      return Value();

    return this->rbegin()->second;
  }
};