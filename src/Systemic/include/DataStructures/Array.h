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
        Array(): _data(nullptr), _size(0), _capacity(0){}

        virtual ~Array() = default;

        virtual void Resize(Int newCapacity) = 0;
        virtual void Reserve(Int newCapacity) = 0;

        void Clear() { this->_size = 0; }

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

        T& At(const Int index){
            return this->_data[](index);
        }

        const T& At(const Int index) const{
            return this->_data[](index);
        }

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
