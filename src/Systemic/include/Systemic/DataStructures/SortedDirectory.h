#pragma once
#include <compare>
#include <Systemic/DataStructures/PolymorphicArray.h>

namespace DataStructures{
    // Sorted array with binary search. Not thread-safe: the owner guards it with its own mutex.
    // TEntry must expose `TKey Key() const`, and TKey must be totally ordered (operator<=>).

    template<typename TKey, typename TEntry>
    class SortedDirectory{
        PolymorphicArray<TEntry> _entries;

        [[nodiscard]] Int32_t LowerBound(const TKey& key) const noexcept{
            Int32_t low = 0;
            Int32_t high = this->_entries.Size();

            while (low < high){
                const auto mid = low + (high - low) / 2;
                if (this->_entries[mid].Key() < key)
                    low = mid + 1;
                else
                    high = mid;
            }

            return high;
        }

        [[nodiscard]] bool IsMatch(const Int32_t index, const TKey& key) const noexcept{
            return index < this->_entries.Size() && this->_entries[index].Key() == key;
        }

    public:
        explicit SortedDirectory()= default;

        explicit SortedDirectory(const ::Memory::IAllocator* allocator, const Int32_t capacity = 20) noexcept
            : _entries(allocator, capacity){}

        void SetAllocator(const ::Memory::IAllocator* allocator){
            this->_entries.SetAllocator(allocator);
        }

        void Reserve(const Int32_t capacity){
            this->_entries.Reserve(capacity);
        }

        [[nodiscard]] const TEntry* Find(const TKey& key) const noexcept{
            const auto index = this->LowerBound(key);
            return this->IsMatch(index, key)
                ? &this->_entries[index]
                : nullptr;
        }

        void Append(TEntry&& entry){
            this->_entries.Push(entry);
        }

        bool Insert(const TEntry& entry){
            const Int32_t index = this->LowerBound(entry.Key());
            if (this->IsMatch(index, entry.Key())) return false;
            this->_entries.Insert(entry, index);
            return true;
        }

        void ReKey(const TKey& oldKey, const TKey& newKey){
            const Int32_t index = this->LowerBound(oldKey);
            assert(this->IsMatch(index, oldKey) && oldKey == newKey);
            this->_entries[index]._key = newKey;
        }

        bool Remove(const TKey& key){
            const Int32_t index = this->LowerBound(key);
            if (!this->IsMatch(index, key)) return false;
            this->_entries.Remove(index);
            return true;
        }

        [[nodiscard]] bool Seal(){
            std::ranges::sort(this->_entries.begin(), this->_entries.end(),
                [](const TEntry& left, const TEntry& right){ return left.Key() < right.Key(); });

            for (Int32_t i = 1; i < this->_entries.Size(); ++i)
                if (this->_entries[i - 1].Key() == this->_entries[i].Key())
                    return false;
            return true;
        }

        [[nodiscard]] const TEntry& operator[](const Int32_t index) const noexcept{
            return this->_entries[index];
        }

        [[nodiscard]] Int32_t Size() const{
            return this->_entries.Size();
        }

        [[nodiscard]] PolymorphicArray<TEntry>::const_iterator begin()const{
            return this->_entries.begin();
        }

        [[nodiscard]] PolymorphicArray<TEntry>::const_iterator end()const{
            return this->_entries.end();
        }
    };
}

