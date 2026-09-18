#pragma once
#include "DataTypes.h"
#include <bit>

template<DataTypes::IsInteger TInput>
class PackedWord {
public:
    TInput _data;

    static constexpr UnsignedInt BITS = sizeof(TInput) * 8;
    static constexpr UnsignedInt SHIFT = std::countr_zero(BITS);
    static constexpr UnsignedInt MASK = BITS - 1;

    PackedWord() = default;
    explicit PackedWord(const TInput data) : _data(data) {}

    [[nodiscard]] static inline constexpr TInput WordIndex(const UnsignedInt index){ return index >> SHIFT; }
    [[nodiscard]] static inline constexpr TInput BitIndex(const UnsignedInt index) { return index & MASK; }

    // Number of words needed for `count` bits.
    [[nodiscard]] static inline constexpr UnsignedInt WordsFor(const UnsignedInt count){ return (count + MASK) >> SHIFT; }

    // Read bit `index` from a bitmap made of TInput words.
    [[nodiscard]] static inline constexpr bool GetBitmapBit(const TInput* words, const UnsignedInt index){
        return GetBit(words[WordIndex(index)], BitIndex(index));
    }

    static inline  constexpr void SetBitmapBit(TInput* words, const UnsignedInt index, const bool value){
        auto& word = words[WordIndex(index)];
        const auto mask = static_cast<TInput>(TInput{1} << BitIndex(index));
        word = static_cast<TInput>((word & ~mask) | (static_cast<TInput>(value) << BitIndex(index)));
    }

    static inline constexpr void OrBitmapBit(TInput* words, const UnsignedInt index, const bool value){
        words[WordIndex(index)] |= static_cast<TInput>(static_cast<TInput>(value) << BitIndex(index));
    }

    template <typename TOut>
    static inline TOut ExtractBits(
        const TInput data,
        const UnsignedTinyInt shift,
        const TInput mask
    ){
        return static_cast<TOut>((data >> shift) & mask);
    }

    template<typename TOut, UnsignedTinyInt Shift, TInput Mask>
    static inline constexpr TOut ExtractBits(const TInput data){
        return static_cast<TOut>((data >> Shift) & Mask);
    }

    static inline void SetBits(
        TInput* data,
        const TInput value,
        const UnsignedTinyInt shift,
        const TInput mask
    ) {
        *data = static_cast<TInput>((*data & ~(mask << shift)) | ((value & mask) << shift));
    }

    template<UnsignedTinyInt Shift, TInput Mask>
    static inline constexpr void SetBits(
        TInput* data,
        const TInput value
    ){
        *data = static_cast<TInput>((*data & ~(Mask << Shift)) | ((value & Mask) << Shift));
    }

    static inline void SetBit(
        TInput* data,
        const UnsignedTinyInt bitPosition,
        const bool value
    ) {
        if (value)
            *data |= static_cast<TInput>(TInput{1} << bitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << bitPosition));
    }

    template<UnsignedTinyInt BitPosition>
    static inline constexpr void SetBit(
        TInput* data,
        const bool value
    ){
        if (value)
            *data |= static_cast<TInput>(TInput{1} << BitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << BitPosition));
    }

    template<UnsignedTinyInt BitPosition, bool Value>
    static inline constexpr void SetBit(
        TInput* data
    ){
        if constexpr (Value)
            *data |= static_cast<TInput>(TInput{1} << BitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << BitPosition));
    }


    template<bool Value>
    static inline constexpr void SetBit(
        TInput* data,
        const TInput bitPosition
    ){
        if constexpr (Value)
            *data |= static_cast<TInput>(TInput{1} << bitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << bitPosition));
    }

    static inline constexpr bool GetBit(
        const TInput data,
        const UnsignedTinyInt bitPosition
    ) {
        return (data & (TInput{1} << bitPosition)) != 0;
    }

    template<TInput BitPosition>
    static inline constexpr bool GetBit(
        const TInput data
    ){
        return (data & (TInput{1} << BitPosition)) != 0;
    }

    static inline constexpr void OrBit(TInput* data, const UnsignedTinyInt bitPosition, const bool value){
        *data |= static_cast<TInput>(static_cast<TInput>(value) << bitPosition);
    }

    static inline constexpr UnsignedTinyInt SIZE = sizeof(TInput);
};

using PackedByte = PackedWord<UnsignedTinyInt>;
using WireBitmap = PackedByte;                   // 8 rows per byte
using EngineBitmap = PackedWord<UnsignedBigInt>;   // 64 rows per word

static_assert(PackedByte::SHIFT == 3 && PackedByte::MASK == 7);
static_assert(EngineBitmap::SHIFT == 6 && EngineBitmap::MASK == 63);
static_assert(PackedByte::WordIndex(17) == 2 && PackedByte::BitIndex(17) == 1);
static_assert(PackedByte::WordsFor(0) == 0 && PackedByte::WordsFor(1) == 1 && PackedByte::WordsFor(9) == 2);
