#pragma once
#include <algorithm>
#include <functional>
#include <stdexcept>
#include <cstring>
#include "../DataTypes/DataTypes.h"
#include "../Memory/IAllocator.h"

namespace DataStructures{
    enum class ArraySortType: UnsignedTinyInt{
        ASC = 0,
        DESC = 1
    };

    template <typename T>
    class PolymorphicArray final{
        const Memory::IAllocator* _allocator;
        T* _data;
        Int _size;
        Int _capacity;

    public:
        PolymorphicArray()
            :    _allocator(nullptr), _data(nullptr),
                _size(0), _capacity(0){}

        explicit PolymorphicArray(const Memory::IAllocator* allocator)
            :   _allocator(allocator), _data(nullptr),
                _size(0), _capacity(0){}

        PolymorphicArray(const Memory::IAllocator* allocator, const Int capacity)
            :   _allocator(allocator), _data(static_cast<T*>(this->_allocator->AllocateRaw(capacity * sizeof(T)))),
                _size(0), _capacity(capacity){}

        PolymorphicArray(const Memory::IAllocator* allocator, Int capacity, T value){
            this->_allocator = allocator;
            this->_data = static_cast<T*>(this->_allocator->AllocateRaw(capacity * sizeof(T)));
            this->_size = capacity;
            this->_capacity = capacity;

            for (Int i = 0; i < capacity; i++)
                this->_data[i] = value;
        }

        PolymorphicArray(const PolymorphicArray& other){
            this->_allocator = other._allocator;
            this->_data = static_cast<T*>(this->_allocator->AllocateRaw(other._capacity * sizeof(T)));
            std::memcpy(this->_data, other._data, other._size * sizeof(T));

            this->_size = other._size;
            this->_capacity = other._capacity;
        }

        PolymorphicArray& operator=(const PolymorphicArray& other){
            if (this == &other)
                return *this;

            this->_allocator = other._allocator;
            this->_data = static_cast<T*>(this->_allocator->AllocateRaw(other._capacity * sizeof(T)));
            std::memcpy(this->_data, other._data, other._size * sizeof(T));

            this->_size = other._size;
            this->_capacity = other._capacity;

            return *this;
        }

        PolymorphicArray(PolymorphicArray&& other) noexcept
            :   _allocator(other._allocator), _data(other._data),
                _size(other._size), _capacity(other._capacity){
            other._data = nullptr;
            other._allocator = nullptr;
            other._size = 0;
            other._capacity = 0;
        }

        PolymorphicArray& operator=(PolymorphicArray&& other) noexcept{
            if (this == &other)
                return *this;

            this->_allocator = other._allocator;
            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;
            other._allocator = nullptr;
            other._size = 0;
            other._capacity = 0;

            return *this;
        }

        PolymorphicArray& PartialMove(PolymorphicArray& other, Int offSet) noexcept{
            if (this == &other)
                return *this;

            this->_allocator = other._allocator;
            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;
            other._allocator = nullptr;
            other._size = 0;
            other._capacity = 0;

            this->_data += offSet;
            return *this;
        }

        void Clear() { this->_size = 0; }

        void SetNewSize(const Int newSize){
            if (newSize > this->_capacity)
                this->Reserve(newSize);
            this->_size = newSize;
        }

        void Push(T&& value){
            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0) ? 1 : this->_capacity * 2;
                this->Reserve(newCapacity);
            }

            this->_data[this->_size++] = std::move(value);
        }

        inline void Push(const T& value){
            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0)
                    ? 1
                    : this->_capacity * 2;
                this->Reserve(newCapacity);
            }

            this->_data[this->_size++] = value;
        }

        void Insert(const T& value, const Int index){
            if (index != 0 && index > this->_size)
                throw std::runtime_error("PolymorphicArray Insert: Index is out of range.");

            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0)
                    ? 1
                    : this->_capacity * 2;
                this->Reserve(newCapacity);
            }

            for (Int i = this->_size - 1; i >= index; --i)
                this->_data[i + 1] = std::move(this->_data[i]);

            this->_data[index] = value;
            ++this->_size;
        }

        void Insert(const T& value, const Int index, const Int count){
            if (index != 0 && index > this->_size)
                throw std::runtime_error("PolymorphicArray Insert: Index is out of range.");

            if (this->_size + count >= this->_capacity){
                if (this->_capacity == 0)
                    this->Reserve(count);
                else{
                    auto newCapacity = this->_capacity * 2;
                    while (newCapacity < this->_size + count)
                        newCapacity *= 2;

                    this->Reserve(newCapacity);
                }
            }
            for (Int i = this->_size - 1; i >= index; --i)
                this->_data[i + count] = std::move(this->_data[i]);

            for (Int i = 0; i < count; ++i)
                this->_data[index + i] = value;

            this->_size += count;
        }

        void AlignSize(){
            this->_size = this->_capacity;
        }

        void MemoryCopy(const void* src, const Int size){
            if (this->_size + size > this->_capacity){
                auto newCapacity = (this->_capacity == 0) ? size : this->_capacity * 2;
                while (newCapacity < this->_size + size) newCapacity *= 2;
                this->Reserve(newCapacity);
            }
            std::memcpy(this->_data + this->_size, src, size);
            this->_size += size;
        }

        void Resize(Int newCapacity){
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            std::memset(newData + this->_size, 0, (newCapacity - this->_size) * sizeof(T));

            this->_data = newData;
            this->_capacity = newCapacity;
            this->_size = newCapacity;
        }

        void Reserve(Int newCapacity){
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));

            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Remove(Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("PolymorphicArray: Index out of range.");

            // Shift elements to the left to fill the gap
            for (Int i = index; i < this->_size - 1; i++)
                this->_data[i] = std::move(this->_data[i + 1]);

            --this->_size;
        }

        void RemoveFrom(Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("PolymorphicArray: Index out of range.");

            this->_size = index;
        }

        T& At(const Int index){
            return this->_data[index];
        }

        const T& At(const Int index) const{
            return this->_data[index];
        }

        T& Start(){
            if (this->_size == 0)
                throw std::runtime_error("Array is empty.");

            return this->_data[0];
        }

        [[nodiscard]] T& operator[](Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("PolymorphicArray: Index out of range.");

            return this->_data[index];
        }

        [[nodiscard]] const T& operator[](Int index) const{
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("PolymorphicArray: Index out of range.");

            return this->_data[index];
        }

        void Pop(){
            this->Remove(this->_size - 1);
        }

        template<typename... Args>
        static PolymorphicArray From(const Memory::IAllocator* allocator, Args&&... args){
            PolymorphicArray arr(allocator, static_cast<Int>(sizeof...(args)));
            (arr.Push(std::forward<Args>(args)), ...);
            return arr;
        }

        void TrySetAllocator(const Memory::IAllocator* allocator){
            if (this->_allocator != nullptr)
                return;

            this->_allocator = allocator;
        }

        void SetAllocator(const Memory::IAllocator* allocator) { this->_allocator = allocator; }

        void SetData(T* data, const Int size){
            this->_data = data;
            this->_size = size;
        }

        [[nodiscard]] const Memory::IAllocator* GetAllocator() const { return this->_allocator; }

        [[nodiscard]] bool HasAllocator() const { return this->_allocator != nullptr; }

        [[nodiscard]] Int Size() const { return this->_size; }
        [[nodiscard]] Int Capacity() const { return this->_capacity; }
        [[nodiscard]] bool Empty() const { return this->_size == 0; }

        [[nodiscard]] T* Data() { return this->_data; }
        [[nodiscard]] const T* Data() const { return this->_data; }

        [[nodiscard]] T* Begin() { return this->_data; }
        [[nodiscard]] T* End() { return this->_data + this->_size; }

        [[nodiscard]] T* Front() { return this->_data; }
        [[nodiscard]] T* Back() { return this->_data + this->_size - 1; }

        [[nodiscard]] const T* Front() const { return this->_data; }
        [[nodiscard]] const T* Back() const { return this->_data + this->_size - 1; }

        //STL Compatibility
        using iterator = T*;
        using const_iterator = const T*;

        iterator begin() { return this->_data; }
        iterator end() { return this->_data + this->_size; }

        const_iterator begin() const { return this->_data; }
        const_iterator end() const { return this->_data + this->_size; }

        const_iterator cbegin() const { return this->_data; }
        const_iterator cend() const { return this->_data + this->_size; }

        iterator erase(iterator pos){
            if (pos < this->begin() || pos >= this->end())
                throw std::out_of_range("DataStructures:PolymorphicArray:erase: Iterator out of range.");

            Int index = pos - this->begin();
            this->Remove(index);
            return this->begin() + index;
        }

        template<ArraySortType TSort>
        void Sort(){
            if constexpr (TSort == ArraySortType::ASC)
                std::sort(this->begin(), this->end(), std::less<T>{});
            else
                std::sort(this->begin(), this->end(), std::greater<T>{});
        }

        template<typename Compare>
        void Sort(Compare compare){
            std::sort(this->begin(), this->end(), std::move(compare));
        }

        template<typename Compare>
        void SortWith(){
            std::sort(this->begin(), this->end(), Compare{});
        }
    };
}
