#pragma once
#include <stdexcept>
#include <initializer_list>
#include <cassert>
#include <Systemic/DataTypes/DataTypes.h>

namespace DataStructures{
    template <typename T, Int N>
    class StaticArray final{
        T _data[N];
        Int _size;
        static_assert(N > 0);

        public:
            // Empty array — N slots allocated on stack, 0 logically used
            constexpr StaticArray() : _data{}, _size(0) {}
            constexpr  ~StaticArray() = default;

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
                assert(size >= 0 && size <= N && "StaticArray: size exceeds capacity");
                std::copy(data, data + size, this->_data);
            }

            // Construct from initializer list
            constexpr StaticArray(std::initializer_list<T> list)
                : _data{}, _size(static_cast<Int>(list.size()))
            {
                assert(static_cast<Int>(list.size()) <= N && "StaticArray: initializer list exceeds capacity");
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
                assert(this->_size < N && "StaticArray: Push exceeds capacity");
                this->_data[this->_size++] = value;
            }

            constexpr void Insert(const Int index, const T& value){
                assert(index >= 0 && index <= this->_size && "StaticArray Insert: Index is out of range.");   // == _size appends
                assert(this->_size < N && "StaticArray Insert: Array is full.");

                for (Int i = this->_size - 1; i >= index; --i)
                    this->_data[i + 1] = this->_data[i];
                this->_data[index] = value;
                ++this->_size;
            }

            constexpr void Insert(const Int index, const Int size, const T& data){
                assert(index >= 0 && index <= this->_size && "StaticArray Insert: Index is out of range.");
                assert(size >= 0 && this->_size + size <= N && "StaticArray Insert: Not enough capacity.");

                for (Int i = this->_size - 1; i >= index; --i)
                    this->_data[i + size] = this->_data[i];

                for (Int i = index; i < index + size; ++i)
                    this->_data[i] = data;

                this->_size += size;
            }

            constexpr void Remove(const Int index){
                assert(index >= 0 && index < this->_size && "StaticArray Remove: Index is out of range.");
                for (Int i = index; i < this->_size - 1; ++i)
                    this->_data[i] = this->_data[i + 1];
                --this->_size;
            }

            // removes [start, end)
            constexpr void Remove(const Int start, const Int end){
                assert(start >= 0 && start <= end && end <= this->_size && "StaticArray Remove: Range is out of bounds.");
                const Int count = end - start;
                for (Int i = start; i < this->_size - count; ++i)
                    this->_data[i] = this->_data[i + count];
                this->_size -= count;
            }

            constexpr void Pop(){
                assert(this->_size > 0 && "StaticArray Pop: Array is empty.");
                --this->_size;
            }

            constexpr void SetData(const T* data, const Int size){
                assert(size >= 0 && size <= N && "StaticArray: size exceeds capacity");
                std::copy(data, data + size, this->_data);
                this->_size = size;
            }

            constexpr void SetSize(const Int size){
                assert(size >= 0 && size <= N && "StaticArray: size exceeds capacity");
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
