#pragma once
#include <cstring>
#include <stdexcept>

#include "../DataTypes/DataTypes.h"

namespace DataStructures{
    template<typename T>
    class Array{
    protected:
        T* _data;
        Int _size;
        Int _capacity;

    public:
        Array(){
            this->_data = nullptr;
            this->_size = 0;
            this->_capacity = 0;
        }

        explicit Array(Int capacity){
            this->_data = static_cast<T*>(std::malloc(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;
        }

        Array(Int capacity, T value){
            this->_data = static_cast<T*>(std::malloc(capacity * sizeof(T)));
            this->_size = 0;
            this->_capacity = capacity;

            //????
            std::memcpy(this->_data, &value, capacity * sizeof(T));
        }

        Array(const Array& other){
            this->_data = static_cast<T*>(std::malloc(other._capacity * sizeof(T)));
            std::memcpy(this->_data, other._data, other._size * sizeof(T));

            this->_size = other._size;
            this->_capacity = other._capacity;
        }

        Array& operator=(const Array& other){
            if (this == &other)
                return *this;

            this->_data = static_cast<T*>(std::malloc(other._capacity * sizeof(T)));
            std::memcpy(this->_data, other._data, other._size * sizeof(T));

            this->_size = other._size;
            this->_capacity = other._capacity;

            return *this;
        }

        Array(Array&& other) noexcept{
            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;
        }

        Array& operator=(Array&& other) noexcept{
            if (this == &other)
                return *this;

            this->_data = other._data;
            this->_size = other._size;
            this->_capacity = other._capacity;

            other._data = nullptr;

            return *this;
        }

        virtual ~Array(){
            std::free(this->_data);
        }

        void Push(T&& value){
            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0) ? 1 : this->_capacity * 2;
                this->Resize(newCapacity);
            }

            this->_data[this->_size++] = std::move(value);
        }

        void Push(const T& value){
            if (this->_size >= this->_capacity){
                auto newCapacity = (this->_capacity == 0) ? 1 : this->_capacity * 2;
                this->Resize(newCapacity);
            }

            this->_data[this->_size++] = value;
        }

        void Remove(Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("Index out of range.");

            // Shift elements to the left to fill the gap
            for (Int i = index; i < this->_size - 1; i++)
                this->_data[i] = std::move(this->_data[i + 1]);

            --this->_size;
        }

        void RemoveFrom(Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("Index out of range.");

            this->_size = index;
        }

        virtual void Resize(Int newCapacity){
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(std::malloc(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        virtual void Reserve(Int newCapacity){
            if (newCapacity <= this->_capacity)
                return;

            T* newData = static_cast<T*>(std::malloc(newCapacity * sizeof(T)));
            std::memcpy(newData, this->_data, this->_size * sizeof(T));
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Clear() { this->_size = 0; }

        T& Start(){
            if (this->_size == 0)
                throw std::runtime_error("Array is empty.");

            return this->_data[0];
        }

        [[nodiscard]] T& operator[](Int index){
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("Index out of range.");

            return this->_data[index];
        }

        [[nodiscard]] const T& operator[](Int index) const{
            if (index < 0 || index >= this->_size)
                throw std::out_of_range("Index out of range.");

            return this->_data[index];
        }

        [[nodiscard]] Int Size() const { return this->_size; }
        [[nodiscard]] Int Capacity() const { return this->_capacity; }
        [[nodiscard]] bool Empty() const { return this->_size == 0; }

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
                throw std::out_of_range("Iterator out of range.");

            Int index = pos - this->begin();
            this->Remove(index);
            return this->begin() + index;
        }
    };
}
