#pragma once
#include "../../../Systemic/include/Encoding.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "../../../Systemic/include/DataTypes/PackedWord.h"

namespace CoreEngine::StorageTypes
{
    class SerializedRow;
}

namespace DataTypes::Indexing {
    struct KeyEntry{
        static constexpr UnsignedSmallInt SIZE_MASK = 0x7FFF;
        static constexpr UnsignedSmallInt NULL_SHIFT = 15;

        UnsignedSmallInt _offset;
        UnsignedSmallInt _meta;

        KeyEntry(): _offset(0), _meta(0){}
        KeyEntry(
            const UnsignedSmallInt offset,
            const UnsignedSmallInt size,
            const bool isNull
        ) : _offset(offset), _meta(EncodeMeta(isNull, size)){}

        [[nodiscard]] bool IsNull() const{
            return PackedWord<UnsignedSmallInt>::GetBit<NULL_SHIFT>(this->_meta);
        }

        [[nodiscard]] UnsignedSmallInt Size() const{
            return PackedWord<UnsignedSmallInt>::ExtractBits<UnsignedSmallInt, 0, SIZE_MASK>(this->_meta);
        }

        [[nodiscard]] UnsignedSmallInt Offset() const{
            return this->_offset;
        }

        static UnsignedSmallInt EncodeMeta(const bool isNull, const UnsignedSmallInt size){
            UnsignedSmallInt meta = 0;
            PackedWord<UnsignedSmallInt>::SetBits<0, SIZE_MASK>(&meta, size);
            PackedWord<UnsignedSmallInt>::SetBit<NULL_SHIFT>(&meta, isNull);
            return meta;
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

        template<IsInteger T>
        [[nodiscard]] T AsInt(Int pos = 0)const;

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
            entry->_meta = KeyEntry::EncodeMeta(false, sizeWritten);
            dataOffset += sizeWritten;
            ++i;
        }(), ...);

        SetCount(buffer, count);
        SetSize(buffer, total);
        this->_data = buffer;
    }

    template <IsInteger T>
    T Key::AsInt(const Int pos) const{
        const auto* entry = this->GetEntry(pos);
        return Encoding::DecodeInteger<T>(this->_data + entry->_offset);
    }
}
