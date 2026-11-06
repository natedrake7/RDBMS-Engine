#pragma once
#include <Systemic/Encoding.h>
#include <Systemic/DataTypes/Value.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/DataTypes/PackedWord.h>

namespace CoreEngine::StorageTypes
{
    class SerializedRow;
}

namespace DataTypes::Indexing {
    struct KeyEntry{
        static constexpr UInt16_t SIZE_MASK = 0x7FFF;
        static constexpr UInt16_t NULL_SHIFT = 15;

        UInt16_t _offset;
        UInt16_t _meta;

        KeyEntry(): _offset(0), _meta(0){}
        KeyEntry(
            const UInt16_t offset,
            const UInt16_t size,
            const bool isNull
        ) : _offset(offset), _meta(EncodeMeta(isNull, size)){}

        [[nodiscard]] bool IsNull() const{
            return PackedWord<UInt16_t>::GetBit<NULL_SHIFT>(this->_meta);
        }

        [[nodiscard]] UInt16_t Size() const{
            return PackedWord<UInt16_t>::ExtractBits<UInt16_t, 0, SIZE_MASK>(this->_meta);
        }

        [[nodiscard]] UInt16_t Offset() const{
            return this->_offset;
        }

        static UInt16_t EncodeMeta(const bool isNull, const UInt16_t size){
            UInt16_t meta = 0;
            PackedWord<UInt16_t>::SetBits<0, SIZE_MASK>(&meta, size);
            PackedWord<UInt16_t>::SetBit<NULL_SHIFT>(&meta, isNull);
            return meta;
        }
    };

    struct Key{
        static constexpr Int32_t HEADER_SIZE = 2 * sizeof(key_size_t);

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

        Key(Key&&) noexcept = default;
        Key& operator=(Key&&) noexcept = default;

        Key(const Key&) = delete;
        Key& operator=(const Key&) = delete;

        [[nodiscard]] inline key_size_t Count() const{
            return *reinterpret_cast<const key_size_t*>(this->_data);
        }

        [[nodiscard]] inline const KeyEntry* GetEntry(const Int32_t index)const{
            return reinterpret_cast<const KeyEntry*>(this->_data + HEADER_SIZE + index * sizeof(KeyEntry));
        }

        [[nodiscard]] inline key_size_t Size()const{
            return *reinterpret_cast<const key_size_t*>(this->_data + sizeof(key_size_t));
        }

        [[nodiscard]] inline bool Empty()const{
            return this->Count() == 0;
        }

        [[nodiscard]] static Comparators::Comparator CompareEntryAt(const Key& lhs, const Key& rhs, Int32_t index);

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
        [[nodiscard]] T AsInt(Int32_t pos = 0)const;

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
        constexpr Int32_t count = sizeof...(Ts);
        const Int32_t total = HEADER_SIZE + count * sizeof(KeyEntry)
                        + static_cast<Int32_t>((0 + ... + sizeof(Ts)));

        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(total));

        Int32_t i = 0;
        Int32_t dataOffset = HEADER_SIZE + count * sizeof(KeyEntry);
        ([&]{
            auto* entry = reinterpret_cast<KeyEntry*>(buffer + HEADER_SIZE + i * sizeof(KeyEntry));

            const auto sizeWritten = Encoding::EncodeInteger<Ts>(buffer + dataOffset, values);

            entry->_offset = static_cast<UInt16_t>(dataOffset);
            entry->_meta = KeyEntry::EncodeMeta(false, sizeWritten);
            dataOffset += sizeWritten;
            ++i;
        }(), ...);

        SetCount(buffer, count);
        SetSize(buffer, total);
        this->_data = buffer;
    }

    template <IsInteger T>
    T Key::AsInt(const Int32_t pos) const{
        const auto* entry = this->GetEntry(pos);
        return Encoding::DecodeInteger<T>(this->_data + entry->_offset);
    }
}
