#pragma once
#include "DataTypes.h"
#include "../Memory/IAllocator.h"
#include "../Comparators.h"

namespace DataTypes{
    class StringValue{
        static constexpr Int PREFIX_SIZE = 4;
        static constexpr Int INLINE_SIZE = 12;

        union{
            struct{
                Int _size;
                char _prefix[PREFIX_SIZE];
                const char* _data;
            } _external;

            struct{
                Int _size;
                char _data[INLINE_SIZE];
            } _inlineVal;
        } _value;

        public:
            StringValue()
                : _value{} {}

            StringValue(const StringValue&) = delete;
            StringValue operator=(const StringValue&) = delete;

            StringValue(StringValue&& other) noexcept
                : _value{other._value} {}

            StringValue& operator=(StringValue&& other) noexcept {
                if (this == &other)
                    return *this;
                _value = other._value;
                return *this;
            }

            StringValue(const ::Memory::IAllocator* allocator, const char* data, const Int size){
                this->_value._inlineVal._size = size;

                auto* copy = static_cast<char*>(allocator->AllocateRaw(size));
                std::memcpy(copy, data, size);
                this->_value._external._data = copy;

                std::memcpy(this->_value._external._prefix, copy, PREFIX_SIZE);
            }

            StringValue(const char* data, const Int size){
                this->_value._inlineVal._size = size;
                std::memcpy(this->_value._inlineVal._data, data, size);
                std::memset(this->_value._inlineVal._data + size, 0, INLINE_SIZE - size);
            }

            static StringValue Create(const ::Memory::IAllocator* allocator, const char* data, const Int size){
                return (size <= INLINE_SIZE)
                    ? StringValue(data, size)
                    : StringValue(allocator, data, size);
            }

            [[nodiscard]] Int Size()const { return  this->_value._inlineVal._size;}
            [[nodiscard]] bool IsInline()const { return this->Size() <= INLINE_SIZE; }
            [[nodiscard]] const char* Data()const{
                return this->IsInline()
                    ? this->_value._inlineVal._data
                    : this->_value._external._data;
            }

            [[nodiscard]] const char* Prefix() const{
                return reinterpret_cast<const char*>(this) + PREFIX_SIZE;
            }

            [[nodiscard]] friend bool operator==(const StringValue& lhs, const StringValue& rhs){
                return Comparators::Equals(lhs, rhs);
            }

            [[nodiscard]] friend bool operator!=(const StringValue& lhs, const StringValue& rhs){
                return !Comparators::Equals(lhs, rhs);
            }

            [[nodiscard]] friend bool operator<(const StringValue& lhs, const StringValue& rhs){
                return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Less;
            }

            [[nodiscard]] friend bool operator>(const StringValue& lhs, const StringValue& rhs){
                return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Greater;
            }

            [[nodiscard]] friend bool operator>=(const StringValue& lhs, const StringValue& rhs){
                return Comparators::Compare(lhs, rhs) >= Comparators::Comparator::Equal;
            }

            [[nodiscard]] friend bool operator<=(const StringValue& lhs, const StringValue& rhs){
                return Comparators::Compare(lhs, rhs) <= Comparators::Comparator::Equal;
            }

            [[nodiscard]] static inline constexpr Int PrefixSize() { return PREFIX_SIZE; }
            [[nodiscard]] static inline constexpr Int InlineSize() { return INLINE_SIZE; }

            friend std::ostream& operator<<(std::ostream& os, const StringValue& value){
                os.write(value.Data(), value.Size());
                return os;
            }
    };
}
