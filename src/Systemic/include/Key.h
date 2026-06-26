#pragma once
#include "Encoding.h"
#include "DataTypes/Value.h"
#include "DataStructures/PolymorphicArray.h"

namespace DataTypes::Indexing {
    struct KeyEntry{
        UnsignedSmallInt _offset;
        UnsignedSmallInt _size: 15;
        UnsignedSmallInt _isNull: 1;

        KeyEntry(): _offset(0), _size(0), _isNull(0) {}
        KeyEntry(
            const UnsignedSmallInt offset,
            const UnsignedSmallInt size,
            const bool isNull
        ) : _offset(offset), _size(size), _isNull(isNull){}

        [[nodiscard]] bool IsNull() const{
            return static_cast<bool>(this->_isNull);
        }

        [[nodiscard]] UnsignedSmallInt Size() const{
            return this->_size;
        }

        [[nodiscard]] UnsignedSmallInt Offset() const{
            return this->_offset;
        }
    };

    struct Key{
        static constexpr Int HEADER_SIZE = 2 * sizeof(key_size_t);

        const object_t* _data;

        explicit Key();
        explicit Key(const object_t* buffer);
        explicit Key(const ::Memory::IAllocator* allocator, const Value& value);
        explicit Key(
            const ::Memory::IAllocator* allocator,
            const DataStructures::PolymorphicArray<Value>& subKeys
        );

        template<typename... Ts>
        requires (sizeof...(Ts) > 0 && (DataTypes::Primitive<Ts> && ...))
        explicit Key(const Memory::IAllocator* allocator, const Ts&... values);

        Key(Key&& otherKey) noexcept;
        Key& operator=(Key&& otherKey) noexcept;

        [[nodiscard]] key_size_t Count() const;
        [[nodiscard]] inline const KeyEntry* GetEntry(Int index)const;

        [[nodiscard]] key_size_t Size()const;

        [[nodiscard]] bool Empty()const;
        [[nodiscard]] static Comparators::Comparator CompareEntryAt(const Key& lhs, const Key& rhs, Int index);

        friend bool operator==(const Key& lhs, const Key& rhs);
        friend bool operator>(const Key& lhs, const Key& rhs);
        friend bool operator<(const Key& lhs, const Key& rhs);
        friend bool operator<=(const Key& lhs, const Key& rhs);
        friend bool operator>=(const Key& lhs, const Key& rhs);

        [[nodiscard]] bool InClosedRange(const Key& minKey, const Key& maxKey) const;
        [[nodiscard]] bool InOpenRange(const Key& minKey, const Key& maxKey) const;

        [[nodiscard]] static bool PartialEqualityCompare(const Key& lhs, const Key& rhs);
        [[nodiscard]] static bool PartialGreaterThan(const Key& lhs, const Key& rhs);

        [[nodiscard]] static Comparators::Comparator Compare(const Key& lhs, const Key& rhs);
        [[nodiscard]] static Comparators::Comparator PartialCompare(const Key& lhs, const Key& rhs);

        [[nodiscard]] Int AsInt(Int pos = 0)const;
        [[nodiscard]] BigInt AsBigInt(Int pos = 0)const;

        static void SetCount(object_t* buffer, key_size_t count);
        static void SetSize(object_t* buffer, key_size_t size);

        void InsertSingleKey(
            const Memory::IAllocator* allocator,
            const Value& value
        );

        void InsertKeys(
            const Memory::IAllocator* allocator,
            const DataStructures::PolymorphicArray<Value>& subKeys
        );

        [[nodiscard]] static key_size_t EncodeValue(object_t* buffer, const Value& value);

        String ToString(const Memory::IAllocator* allocator) const;
        friend std::ostream& operator<<(std::ostream& os, const Key& key);
    };

    template <typename ... Ts> requires (sizeof...(Ts) > 0 && (DataTypes::Primitive<Ts> && ...))
    Key::Key(const Memory::IAllocator* allocator, const Ts&... values)
        : _data(nullptr){
        constexpr Int count = sizeof...(Ts);
        const Int total = HEADER_SIZE + count * sizeof(KeyEntry)
                        + static_cast<Int>((0 + ... + sizeof(Ts)));

        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(total));

        Int i = 0;
        Int dataOffset = HEADER_SIZE + count * sizeof(KeyEntry);
        ([&]{
            auto* entry = reinterpret_cast<KeyEntry*>(buffer + HEADER_SIZE + i * sizeof(KeyEntry));

            const auto sizeWritten = Encoding::EncodeInteger<Ts>(buffer + dataOffset, values);

            entry->_offset = static_cast<UnsignedSmallInt>(dataOffset);
            entry->_size = sizeWritten;
            entry->_isNull = 0;
            dataOffset += sizeWritten;
            ++i;
        }(), ...);

        SetCount(buffer, count);
        SetSize(buffer, total);
        this->_data = buffer;
    }
}
