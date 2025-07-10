#pragma once

#include <unordered_map>
#include <vector>

using namespace std;

template<typename Key, typename Value>
class Dictionary : public std::unordered_map<Key, Value>
{
    public:
        Dictionary() : std::unordered_map<Key, Value>() {}

        Dictionary(const initializer_list<pair<const Key, Value>>& values) : unordered_map<Key, Value>(values) {}

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
};