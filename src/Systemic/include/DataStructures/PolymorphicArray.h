#pragma once
#include <cstring>
#include "Array.h"
#include "../DataTypes/DataTypes.h"
#include "../Memory/Allocator.h"

namespace DataStructures{
    template <typename T>
    class PolymorphicArray final : public Array<T>{
        const Memory::Allocator* _allocator;

    public:
        PolymorphicArray(){
            this->_allocator = nullptr;

            this->_data = nullptr;
            this->_size = 0;
            this->_capacity = 0;
        }

        explicit PolymorphicArray(const Memory::Allocator& allocator){
            this->_allocator = &allocator;
            this->_data = nullptr;
            this->_size = 0;
            this->_capacity = 0;
        }

        PolymorphicArray(const Memory::Allocator& allocator, Int capacity){
            this->_allocator = &allocator;
            this->_data = static_cast<T*>(this->_allocator->Allocate(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;
        }

        PolymorphicArray(const Memory::Allocator& allocator, Int capacity, T value){
            this->_allocator = &allocator;
            this->_data = static_cast<T*>(this->_allocator->Allocate(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;

            //????
            std::memcpy(this->_data, &value, capacity * sizeof(T));
        }

        PolymorphicArray(const PolymorphicArray& other){
            this->_allocator = other._allocator;
            this->_data = static_cast<T*>(this->_allocator->Allocate(other._capacity * sizeof(T)));
            std::memcpy(this->_data, other._data, other._size * sizeof(T));

            this->_size = other._size;
            this->_capacity = other._capacity;
        }

        PolymorphicArray& operator=(const PolymorphicArray& other){
            if (this == &other)
                return *this;

            this->_allocator = other._allocator;
            this->_data = static_cast<T*>(this->_allocator->Allocate(other._capacity * sizeof(T)));
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

        ~PolymorphicArray() override{
            this->_data = nullptr;
            // this->_allocator->Deallocate(this->_data);
        }

        void Resize(Int newCapacity) override{
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->Allocate(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Reserve(Int newCapacity) override{
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(this->_allocator->Allocate(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void TrySetAllocator(const Memory::Allocator& allocator){
            if (this->_allocator != nullptr)
                return;

            this->_allocator = &allocator;
        }

        void SetAllocator(const Memory::Allocator& allocator) { this->_allocator = &allocator; }
    };
}
