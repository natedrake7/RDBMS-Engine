#pragma once
#include <unordered_set>
#include <vector>

template <typename T>
class HashSet : public std::unordered_set<T>
{
    public:
        HashSet() : std::unordered_set<T>() {}
        
        HashSet(const std::initializer_list<T>& values) : std::unordered_set<T>(values) {}
        
        explicit HashSet(const std::vector<T>& values) : std::unordered_set<T>(values.begin(), values.end()) {}
        
        bool TryGetValue(const T& key, T& value)
        {
            auto it = this->find(key);
            if(it != this->end())
            {
                value = *it;
                return true;
            }

            return false;
        }

        bool Contains(const T& key) const
        {
            return this->find(key) != this->end();
        }
        
        void Remove(const T& key)
        {
            this->erase(key);
        }
        
        void Add(const T& key)
        {
            this->insert(key);
        }

        [[nodiscard]] size_t Size()const { return this->size(); }
};