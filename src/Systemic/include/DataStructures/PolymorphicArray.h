#pragma once
#include <cstring>
#include "Array.h"
#include "../DataTypes/DataTypes.h"
#include "../Memory/IAllocator.h"

namespace DataStructures{
    template <typename T>
    class PolymorphicArray final : public Array<T>{
        const Memory::IAllocator* _allocator;

    public:
        PolymorphicArray(){
            this->_allocator = nullptr;

            this->_data = nullptr;
            this->_size = 0;
            this->_capacity = 0;
        }

        explicit PolymorphicArray(const Memory::IAllocator* allocator){
            this->_allocator = allocator;
            this->_data = nullptr;
            this->_size = 0;
            this->_capacity = 0;
        }

        PolymorphicArray(const Memory::IAllocator* allocator, Int capacity){
            this->_allocator = allocator;
            this->_data = static_cast<T*>(this->_allocator->AllocateRaw(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;
        }

        PolymorphicArray(const Memory::IAllocator* allocator, Int capacity, T value){
            this->_allocator = allocator;
            this->_data = static_cast<T*>(this->_allocator->AllocateRaw(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;

            //????
            std::memcpy(this->_data, &value, capacity * sizeof(T));
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

        PolymorphicArray(PolymorphicArray&& other) noexcept{
            this->_allocator = other._allocator;
            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;
        }

        PolymorphicArray& operator=(PolymorphicArray&& other) noexcept{
            if (this == &other)
                return *this;

            this->_allocator = other._allocator;
            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;

            return *this;
        }

        ~PolymorphicArray() override = default;

        void Resize(Int newCapacity) override{
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));

            // for (Int i = 0; i < this->_size; ++i)
            //     this->_data[i] = std::move(newData[i]);

            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Reserve(Int newCapacity) override{
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Insert(const T& value, const Int index){
            if (index >= this->_size)
                throw std::runtime_error("PolymorphicArray Insert: Index is out of range.");

            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0)
                    ? 1
                    : this->_capacity * 2;
                this->Resize(newCapacity);
            }

            for (Int i = this->_size - 1; i >= index; --i)
                this->_data[i + 1] = std::move(this->_data[i]);

            this->_data[index] = value;
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

        [[nodiscard]] const Memory::IAllocator* GetAllocator() const { return this->_allocator; }

        [[nodiscard]] bool HasAllocator() const { return this->_allocator != nullptr; }
    };
}
