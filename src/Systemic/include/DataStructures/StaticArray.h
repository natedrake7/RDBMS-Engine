#pragma once
#include <cstring>
#include <initializer_list>

#include "../DataTypes/DataTypes.h"

namespace DataStructures{
    template <typename T, Int N>
    class StaticArray final{
        T _data[N];
        Int _size;
        static_assert(N > 0);

        public:
            constexpr StaticArray() : _data(N), _size(N) {}
            explicit constexpr StaticArray(const T* _data, const Int size)
                : _data(size), _size(size){

                if (size > N)
                    throw std::runtime_error("Array size is greater than array capacity");

                std::memcpy(this->_data, _data, size * sizeof(T));
            }
            constexpr StaticArray(std::initializer_list<T> list){
                static_assert(list.size() <= N, "List size is greater than array size");
                std::memcpy(this->_data, list.begin(), list.size() * sizeof(T));
                this->_size = list.size();
            }

            constexpr T& operator[](const Int index){ return this->_data[index]; }
            [[nodiscard]] constexpr Int Size() const{ return this->_size; }

            constexpr const T* Data() const{ return this->_data; }

            constexpr void SetData(const T* data, const Int size){
                std::memcpy(this->_data, data, size * sizeof(T));
                this->_size = size;
            }

            void SetData(const std::vector<T>& data){
                const auto size = data.size();

                if (size > N)
                    throw std::runtime_error("Array size is greater than array capacity");
                std::memcpy(this->_data, data.data(), size * sizeof(T));
                this->_size = size;
            }

            constexpr bool Empty() const{ return this->_size == 0; }

            using iterator = T*;
            using const_iterator = const T*;

            iterator begin() { return this->_data; }
            iterator end() { return this->_data + this->_size; }

            const_iterator begin() const { return this->_data; }
            const_iterator end() const { return this->_data + this->_size; }
    };
}
