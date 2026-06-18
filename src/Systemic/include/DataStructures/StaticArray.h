#pragma once
#include <stdexcept>
#include <initializer_list>

#include "../DataTypes/DataTypes.h"

namespace DataStructures{
    template <typename T, Int N>
    class StaticArray final{
        T _data[N];
        Int _size;
        static_assert(N > 0);

        public:
            // Empty array — N slots allocated on stack, 0 logically used
            constexpr StaticArray() : _data{}, _size(0) {}

            // Fill `size` elements with `value`
            explicit constexpr StaticArray(const Int size, const T& value = T())
                : _data{}, _size(size)
            {
                if (size > N)
                    throw std::runtime_error("StaticArray: size exceeds capacity");
                for (Int i = 0; i < size; i++)
                    _data[i] = value;
            }

            // Construct from raw pointer
            explicit constexpr StaticArray(const T* data, const Int size)
                : _data{}, _size(size){
                if (size > N)
                    throw std::runtime_error("StaticArray: size exceeds capacity");
                std::copy(data, data + size, this->_data);
            }

            // Construct from initializer list
            constexpr StaticArray(std::initializer_list<T> list)
                : _data{}, _size(static_cast<Int>(list.size()))
            {
                if (static_cast<Int>(list.size()) > N)
                    throw std::runtime_error("StaticArray: initializer list exceeds capacity");
                std::copy(list.begin(), list.end(), this->_data);
            }

            constexpr T& operator[](const Int index){ return this->_data[index]; }
            constexpr const T& operator[](const Int index) const { return this->_data[index]; }

            [[nodiscard]] constexpr Int Size() const { return this->_size; }
            [[nodiscard]] static constexpr Int Capacity() { return N; }

            constexpr const T* Data() const { return this->_data; }
            constexpr T* Data() { return this->_data; }

            // Push a single element — used by Decimal byte-by-byte construction
            constexpr void Push(const T& value){
                if (this->_size >= N)
                    throw std::runtime_error("StaticArray: Push exceeds capacity");
                this->_data[this->_size++] = value;
            }

            constexpr void Insert(const Int index, const T& value){
                if (index >= this->_size)
                    throw std::runtime_error("StaticArray Insert: Index is out of range.");

                if (this->_size >= N)
                    throw std::runtime_error("StaticArray Insert: Array is full.");

                for (Int i = this->_size - 1; i >= index; --i)
                    this->_data[i + 1] = this->_data[i];
                this->_data[index] = value;
                ++this->_size;
            }

            constexpr void Insert(const Int index, const Int size, const T& data){
                if (index >= this->_size)
                    throw std::runtime_error("StaticArray Insert: Index is out of range.");
                if (this->_size + size > N)
                    throw std::runtime_error("StaticArray Insert: Array is full.");

                for (Int i = this->_size - 1; i >= index; --i)
                    this->_data[i + size] = this->_data[i];

                for (Int i = index; i < index + size; ++i)
                    this->_data[i] = data;

                this->_size += size;
            }

            constexpr void Remove(const Int index){
                if (index >= this->_size)
                    throw std::runtime_error("StaticArray Remove: Index is out of range.");
                for (Int i = index; i < this->_size - 1; ++i)
                    this->_data[i] = this->_data[i + 1];
                --this->_size;
            }

            constexpr void  Remove(const Int start, const Int end){
                if (start < 0 || start >= this->_size || end >= this->_size)
                    throw std::runtime_error("StaticArray Remove: Index is out of range.");
                const Int count = end - start;
                for (Int i = start; i < this->_size - count; ++i)
                    this->_data[i] = this->_data[i + count];
                this->_size -= count;
            }

            constexpr void Pop(){
                if (this->_size == 0)
                    throw std::runtime_error("StaticArray Pop: Array is empty.");
                --this->_size;
            }

            constexpr void SetData(const T* data, const Int size){
                if (size > N)
                    throw std::runtime_error("StaticArray: size exceeds capacity");
                std::copy(data, data + size, this->_data);
                this->_size = size;
            }

            constexpr void SetSize(const Int size){
                if (size > N)
                    throw std::runtime_error("StaticArray: size exceeds capacity");
                this->_size = size;
            }

            [[nodiscard]] constexpr bool Empty() const { return this->_size == 0; }

            constexpr T First() const { return this->_data[0]; }
            constexpr T Last() const { return this->_data[this->_size - 1]; }

            using iterator = T*;
            using const_iterator = const T*;

            constexpr iterator begin() { return this->_data; }
            constexpr iterator end()   { return this->_data + this->_size; }

            constexpr const_iterator begin() const { return this->_data; }
            constexpr const_iterator end()   const { return this->_data + this->_size; }
    };
}
