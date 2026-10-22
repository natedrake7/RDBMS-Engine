#pragma once
#include <algorithm>
#include <cassert>
#include <functional>
#include <cstring>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Memory/IAllocator.h>
#include <memory>

namespace DataStructures{
    enum class ArraySortType: UnsignedTinyInt{
        ASC = 0,
        DESC = 1
    };

    template <typename T>
    class PolymorphicArray final{
        static constexpr bool TRIVIALLY_COPIABLE = std::is_trivially_copyable_v<T>;

        const Memory::IAllocator* _allocator;
        T* _data;
        Int _size;
        Int _capacity;

        static void Relocate(T* to, T* from, const Int count){
            if (count == 0)
                return;

            if constexpr (TRIVIALLY_COPIABLE)
                std::memcpy(static_cast<void*>(to), static_cast<const void*>(from), count * sizeof(T));
            else{
                std::uninitialized_move_n(from, count, to);
                std::destroy_n(from, count);
            }
        }

        static void Copy(T* to, const T* from, const Int count){
            if (count == 0)
                return;

            if constexpr (TRIVIALLY_COPIABLE)
                std::memcpy(static_cast<void*>(to), static_cast<const void*>(from), count * sizeof(T));
            else
                std::uninitialized_copy_n(from, count, to);
        }

        void GrowIfFull(){
            assert(this->_allocator != nullptr && "PolymorphicArray:GrowIfFull: array growth called by no allocator was present");

            if (this->_size >= this->_capacity)
                this->Reserve(this->_capacity == 0 ? 1 : this->_capacity * 2);
        }

        void GrowTo(const Int required){
            assert(this->_allocator != nullptr && "PolymorphicArray:GrowTo: array growth called by no allocator was present");

            if (required <= this->_capacity)
                return;
            auto newCapacity = this->_capacity == 0 ? 1 : this->_capacity;
            while (newCapacity < required)
                newCapacity *= 2;
            this->Reserve(newCapacity);
        }

    public:
        PolymorphicArray()
            :    _allocator(nullptr), _data(nullptr),
                _size(0), _capacity(0){}

        explicit PolymorphicArray(const Memory::IAllocator* allocator)
            :   _allocator(allocator), _data(nullptr),
                _size(0), _capacity(0){}

        PolymorphicArray(const Memory::IAllocator* allocator, const Int capacity)
            :   _allocator(allocator), _data(static_cast<T*>(this->_allocator->AllocateRaw(capacity * sizeof(T), alignof(T)))),
                _size(0), _capacity(capacity){}

        PolymorphicArray(const Memory::IAllocator* allocator, const Int capacity, T value)
            :   _allocator(allocator), _data(static_cast<T*>(allocator->AllocateRaw(capacity * sizeof(T), alignof(T)))),
                _size(capacity), _capacity(capacity){
            std::uninitialized_fill_n(this->_data, capacity, value);
        }

        PolymorphicArray(const PolymorphicArray& other)
            :   _allocator(other._allocator),
                _data(other._capacity == 0
                        ? nullptr
                        : static_cast<T*>(other._allocator->AllocateRaw(other._capacity * sizeof(T), alignof(T)))
                ),
                _size(other._size), _capacity(other._capacity){
            PolymorphicArray::Copy(this->_data, other._data, other._size);
        }

        PolymorphicArray& operator=(const PolymorphicArray& other){
            if (this == &other)
                return *this;

            std::destroy_n(this->_data, this->_size);
            this->_allocator = other._allocator;

            this->_data = other._capacity == 0
                    ? nullptr
                    : static_cast<T*>(this->_allocator->AllocateRaw(other._capacity * sizeof(T), alignof(T)));

            PolymorphicArray::Copy(this->_data, other._data, other._size);

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

            std::destroy_n(this->_data, this->_size);

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

        PolymorphicArray& PartialMove(PolymorphicArray& other, Int offSet) noexcept requires TRIVIALLY_COPIABLE{
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

        ~PolymorphicArray(){
            if constexpr (!std::is_trivially_destructible_v<T>)
                std::destroy_n(this->_data, this->_size);
        }

        void Clear(){
            std::destroy_n(this->_data, this->_size);
            this->_size = 0;
        }

        void SetNewSize(const Int newSize){
            this->Resize(newSize);
        }

        template<typename... Args>
        T& Emplace(Args&&... args){
            assert(this->_allocator != nullptr && "PolymorphicArray:Emplace: array growth called by no allocator was present");

            if (this->_size < this->_capacity)
                return *std::construct_at(this->_data + this->_size++, std::forward<Args>(args)...);

            const auto newCapacity = this->_capacity == 0
                ? 1
                : this->_capacity * 2;

            auto* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T), alignof(T)));
            auto* element = std::construct_at(newData + this->_size, std::forward<Args>(args)...);

            Relocate(newData, this->_data, this->_size);

            this->_data = newData;
            this->_capacity = newCapacity;

            ++this->_size;

            return *element;
        }

        void Push(T&& value){
            this->Emplace(std::move(value));
        }

        void Push(const T& value){
            this->Emplace(value);
        }

        void Insert(T value, const Int index){
            assert(index >= 0 && index <= this->_size && "PolymorphicArray::Insert: index out of range");

            this->GrowIfFull();

            auto* const slot = this->_data + index;
            auto* const oldEnd = this->_data + this->_size;

            if (index == this->_size)
                std::construct_at(slot, std::move(value));
            else if constexpr (TRIVIALLY_COPIABLE){
                std::memmove(static_cast<void*>(slot + 1), slot, (this->_size - index) * sizeof(T));
                *slot = value;
            }
            else{
                std::construct_at(oldEnd, std::move(*(oldEnd - 1)));
                std::move_backward(slot, oldEnd - 1, oldEnd);
                *slot = std::move(value);
            }

            ++this->_size;
        }

        void Insert(const T& value, const Int index, const Int count){
            assert(!(index != 0 && index > this->_size) && "PolymorphicArray Insert: Index is out of range.");

            if (count == 0)
                return;

            const T copy = value;                                   // `value` may point into this array
            this->GrowTo(this->_size + count);

            auto* const first = this->_data + index;
            auto* const oldEnd = this->_data + this->_size;
            const Int tail = this->_size - index;                   // elements that move right by `count`

            if constexpr (TRIVIALLY_COPIABLE){
                std::memmove(static_cast<void*>(first + count), first, tail * sizeof(T));
                std::uninitialized_fill_n(first, count, copy);
            }
            else if (tail <= count){
                // Every tail element lands past the old end (raw); the gap spans both live and raw slots
                std::uninitialized_move(first, oldEnd, first + count);   // tail      -> [index+count, size+count)  raw
                std::uninitialized_fill(oldEnd, first + count, copy);    // gap, raw  -> [size, index+count)
                std::fill(first, oldEnd, copy);                          // gap, live -> [index, size)  (moved-from)
            }
            else{
                // Only the last `count` tail elements land in raw memory; the gap is entirely live
                std::uninitialized_move(oldEnd - count, oldEnd, oldEnd); // [size-count, size) -> [size, size+count)  raw
                std::move_backward(first, oldEnd - count, oldEnd);       // [index, size-count) -> [index+count, size) live
                std::fill_n(first, count, copy);                         // gap [index, index+count)  live (moved-from)
            }

            this->_size += count;
        }

        void AlignSize(){
            this->Resize(this->_capacity);
        }

        void MemoryCopy(const void* src, const Int size) requires TRIVIALLY_COPIABLE{
            this->GrowTo(this->_size + size);
            PolymorphicArray::Copy(this->_data + this->_size, static_cast<const T*>(src), size);
            this->_size += size;
        }

        void Resize(Int newSize){
            if (newSize < this->_size){
                std::destroy_n(this->_data + newSize, this->_size - newSize);
                this->_size = newSize;
                return;
            }

            this->GrowTo(newSize);
            std::uninitialized_value_construct_n(this->_data + this->_size, newSize - this->_size);   // zeros for scalars
            this->_size = newSize;
        }

        void Reserve(Int newCapacity){
            if (newCapacity <= this->_capacity)
                return;

            auto* newData = static_cast<T*>(this->_allocator->AllocateRaw(newCapacity * sizeof(T), alignof(T)));
            Relocate(newData, this->_data, this->_size);
            this->_data = newData;
            this->_capacity = newCapacity;
        }

        void Remove(Int index){
            assert(!(index < 0 || index >= this->_size) && "PolymorphicArray::Remove: Index out of range.");
            // Shift elements to the left to fill the gap
            for (Int i = index; i < this->_size - 1; i++)
                this->_data[i] = std::move(this->_data[i + 1]);
            std::destroy_at(this->_data + --this->_size);
        }

        void RemoveFrom(Int index){
            assert(!(index < 0 || index >= this->_size) && "PolymorphicArray::RemoveFrom: Index out of range.");
            std::destroy_n(this->_data + index, this->_size - index);
            this->_size = index;
        }

        T& At(const Int index){
            return this->_data[index];
        }

        const T& At(const Int index) const{
            return this->_data[index];
        }

        T& Start(){
            assert(!(this->_size == 0) && "PolymorphicArray::Start: Array is empty.");
            return this->_data[0];
        }

        [[nodiscard]] T& operator[](Int index) noexcept{
            assert(!(index < 0 || index >= this->_size) && "PolymorphicArray::operator[]: Index out of range.");
            return this->_data[index];
        }

        [[nodiscard]] const T& operator[](Int index) const noexcept{
            assert(!(index < 0 || index >= this->_size) && "PolymorphicArray::operator[]const: Index out of range.");
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

        void SetData(T* data, const Int size) requires TRIVIALLY_COPIABLE{
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
            assert(!(pos < this->begin() || pos >= this->end()) && "DataStructures:PolymorphicArray:erase: Iterator out of range.");
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
