#pragma once
#include <unordered_set>
#include <vector>

using namespace std;

template <typename T>
class HashSet : public unordered_set<T>
{
    public:
        HashSet() : unordered_set<T>() {}
        
        HashSet(const initializer_list<T>& values) : unordered_set<T>(values) {}
        
        HashSet(const vector<T>& values) : unordered_set<T>(values.begin(), values.end()) {}
        
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
};