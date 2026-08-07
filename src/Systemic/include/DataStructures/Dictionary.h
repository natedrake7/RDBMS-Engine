#pragma once
#include <unordered_map>
#include <vector>

#include "PolymorphicArray.h"

template<typename Key, typename Value>
class Dictionary : public std::unordered_map<Key, Value>{
    public:
        Dictionary() : std::unordered_map<Key, Value>() {}

        Dictionary(const std::initializer_list<std::pair<const Key, Value>>& values) : std::unordered_map<Key, Value>(values) {}

        bool TryGetValue(const Key& key, Value& value) const{
            auto it = this->find(key);
            if (it != this->end()) 
            {
                value = it->second;
                return true;
            }

            return false;
        }

        bool TryGetValue(const Key& key, Value*& value){
            auto it = this->find(key);
            if (it != this->end())
            {
                value = &it->second;
                return true;
            }

            return false;
        }

        bool TryGetValue(const Key& key, Value*& value) const{
            auto it = this->find(key);
            if (it != this->end())
            {
                value = &it->second;
                return true;
            }

            return false;
        }

        bool Contains(const Key& key) const {
            return this->find(key) != this->end();
        }

        void Remove(const Key& key) {
            this->erase(key);
        }

        void Add(const Key& key, const Value& value){
            this->insert(std::make_pair(key, value));
        }

        void Add(const Key& key, Value&& value){
            this->insert(std::make_pair(key, std::move(value)));
        }

        void ForceAdd(const Key& key, const Value& value) {
            if (this->Contains(key))
                this->Remove(key);

            this->Add(key, value);
        }

        void ForceAdd(const Key& key, Value&& value) {
            if (this->Contains(key))
                this->Remove(key);

            this->Add(key, std::move(value));
        }

        void Update(const Key& key, const Value& value)
        {
            if (!this->Contains(key))
                return;

            this->at(key) = value;
        }

        void Update(const Key& key, Value&& value){
            if (!this->Contains(key))
                return;

            this->at(key) = std::move(value);
        }

        void AddOrUpdate(const Key& key, const Value& value) {
            if (!this->Contains(key)) {
                this->Add(key, value);
                return;
            }

            this->Update(key, value);
        }

        void AddOrUpdate(const Key& key, Value&& value) {
            if (!this->Contains(key)) {
                this->Add(key, std::move(value));
                return;
            }

            this->Update(key, std::move(value));
        }

        Value& Get(const Key& key){
            return this->at(key);
        }

        const Value& Get(const Key& key) const{
            return this->at(key);
        }

        std::vector<Value> ToVector() const{
            std::vector<Value> values;
            for (const auto& pair : *this)
                values.push_back(pair.second);

            return values;
        }

        DataStructures::PolymorphicArray<Value> ToPolymorphicArray(const ::Memory::IAllocator* allocator) const{
            DataStructures::PolymorphicArray<Value> array(allocator, this->size());
            for (const auto& pair : *this)
                array.Push(pair.second);

            return array;
        }

        static Dictionary FromVector(const std::vector<Key>& items, const Value& defaultValue) {
            Dictionary dict;
            for (const auto& item : items)
                dict.Add(item, defaultValue);
            return dict;
        }

        static Dictionary FromArray(const DataStructures::PolymorphicArray<Key>& items, const Value& defaultValue) {
            Dictionary dict;
            for (const auto& item : items)
                dict.Add(item, defaultValue);
            return dict;
        }
};

