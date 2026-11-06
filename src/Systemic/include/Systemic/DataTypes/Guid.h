#pragma once
#include <array>
#include <cstdint>
#include <random>
#include <string>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/DataTypes/StringView.h>
#include <Systemic/DataStructures/StaticArray.h>

namespace DataTypes {
    inline constexpr data_size_t GUID_SIZE = 16;
    inline constexpr data_size_t GUID_STRING_SIZE = 36;
    inline constexpr data_size_t GUID_STRING_SIZE_WITH_HYPHENS = GUID_STRING_SIZE + 2;

    namespace GuidDetail{
        inline constexpr char HEX_DIGITS[] = "0123456789abcdef";
        inline constexpr UInt8_t INVALID_HEX = 0xFF;

        inline constexpr auto HEX_VALUES = []{
            std::array<UInt8_t, 256> table{};
            table.fill(INVALID_HEX);
            for (Int32_t i = 0; i < 10; i++)
                table['0' + i] = static_cast<UInt8_t>(i);
            for (Int32_t i = 0; i < 6; i++){
                table['a' + i] = static_cast<UInt8_t>(10 + i);
                table['A' + i] = static_cast<UInt8_t>(10 + i);
            }
            return table;
        }();

        inline constexpr auto HYPHENS_POSITIONS = std::array<UInt8_t, 4>{
            8,13,18,23
        };

        // Byte positions that have a hyphen before them in the text form
        [[nodiscard]] constexpr bool HyphenBefore(const Int32_t byteIndex){
            return byteIndex == 4 || byteIndex == 6 || byteIndex == 8 || byteIndex == 10;
        }

    }

    class Guid {
        UInt8_t _data[GUID_SIZE];

        using StringBuffer = DataStructures::StaticArray<char, GUID_STRING_SIZE>;

        [[nodiscard]] inline constexpr UInt64_t Word(const Int32_t index)const noexcept{
            const auto start = index * static_cast<Int32_t>(sizeof(UInt64_t));

            if consteval{
                UInt64_t word = 0;
                for (Int32_t i = 0; i < static_cast<Int32_t>(sizeof(word)); i++)
                    word = (word << 8) | this->_data[start + i];
                return word;
            }
            else{
                UInt64_t word;
                std::memcpy(&word, this->_data + start, sizeof(word));
                return std::byteswap(word);
            }

            std::unreachable();
        }

    public:
        constexpr explicit Guid()
            : _data(){}
        explicit constexpr Guid(const object_t* data)
            : _data(){
            if consteval{
                for (Int32_t i = 0; i < GUID_SIZE; i++)
                    this->_data[i] = data[i];
            }
            else{
                std::memcpy(this->_data, data, GUID_SIZE);
            }
        }

        [[nodiscard]] inline constexpr UInt8_t* Data(){
            return this->_data;
        }

        [[nodiscard]] inline constexpr const UInt8_t* Data() const{
            return this->_data;
        }

        [[nodiscard]] inline constexpr StringBuffer ToStringBuffer() const{
            StringBuffer buffer(GUID_STRING_SIZE);
            auto* out = buffer.Data();

            for (Int32_t i = 0; i < GUID_SIZE; i++){
                if (GuidDetail::HyphenBefore(i))
                    *out++ = '-';
                *out++ = GuidDetail::HEX_DIGITS[this->_data[i] >> 4];
                *out++ = GuidDetail::HEX_DIGITS[this->_data[i] & 0x0F];
            }

            return buffer;
        }

        [[nodiscard]] String ToString(const ::Memory::IAllocator* allocator) const;
        [[nodiscard]] StringValue ToStringValue(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] inline static constexpr bool TryParse(const StringView& str, Guid* out){
            const auto* text = str.Data();
            auto size = str.Size();

            if (size == GUID_STRING_SIZE_WITH_HYPHENS){
                if (text[0] != '{' || text[size - 1] != '}')
                    return false;
                text++;
                size -= 2;
            }

            if (size != GUID_STRING_SIZE)
                return false;

            bool hasAllHyphens = true;
            for (const auto pos: GuidDetail::HYPHENS_POSITIONS)
                hasAllHyphens &= text[pos] == '-';
            if (!hasAllHyphens)
                return false;

            UInt8_t bytes[GUID_SIZE]{};
            UInt8_t seen = 0;
            Int32_t position = 0;

            for (Int32_t i = 0; i < GUID_SIZE; i++){
                position += GuidDetail::HyphenBefore(i);

                const auto high = GuidDetail::HEX_VALUES[static_cast<unsigned char>(text[position])];
                const auto low = GuidDetail::HEX_VALUES[static_cast<unsigned char>(text[position + 1])];
                position += 2;

                seen |= high | low;
                bytes[i] = static_cast<UInt8_t>((high << 4) | low);
            }

            if (seen > 0x0F)
                return false;

            *out = Guid(bytes);
            return true;
        }

        [[nodiscard]] inline static constexpr Guid Parse(const StringView& str){
            Guid out;
            if (!Guid::TryParse(str, &out))
                throw std::invalid_argument("Guid::Parse: invalid format");
            return out;
        }

        [[nodiscard]] inline static constexpr bool Validate(const StringView& str){
            Guid ignoredOut;
            return Guid::TryParse(str, &ignoredOut);
        }

        [[nodiscard]] inline static constexpr Guid Empty(){
            return Guid();
        }

        [[nodiscard]] inline constexpr long double Interpolate() const{
            return static_cast<long double>(this->Word(0));
        }

        [[nodiscard]] constexpr std::size_t Hash() const noexcept{
            const auto mixed = (this->Word(0) * 0x9E3779B97F4A7C15ULL) ^ this->Word(1);
            return static_cast<std::size_t>(mixed ^ (mixed >> 32));
        }

        [[nodiscard]] inline friend constexpr bool operator==(const Guid& lhs, const Guid& rhs) noexcept{
            return __builtin_memcmp(lhs._data, rhs._data, GUID_SIZE) == 0;
        }

        [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const Guid& lhs, const Guid& rhs) noexcept{
            const auto left = lhs.Word(0), right = rhs.Word(0);
            return left != right
                ? left <=> right
                : lhs.Word(1) <=> rhs.Word(1);
        }

        friend std::ostream& operator<<(std::ostream& os, const Guid& guid);
        static std::mt19937_64& ThreadGenerator();

        [[nodiscard]] static Guid NewGuid();
        [[nodiscard]] static Guid NewSequentialGuid();
    };

    static_assert(sizeof(Guid) == GUID_SIZE && alignof(Guid) == 1);
    static_assert(std::is_trivially_copyable_v<Guid> && std::is_trivially_destructible_v<Guid>);
}

    template<>
    struct std::hash<DataTypes::Guid> {
        size_t operator()(const DataTypes::Guid& guid) const noexcept {
            return guid.Hash();
        }
    };