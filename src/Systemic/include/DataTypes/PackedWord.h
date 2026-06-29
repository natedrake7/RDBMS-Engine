#pragma once
#include "DataTypes.h"

template<DataTypes::IsInteger TInput>
class PackedWord {
public:
    TInput _data;

    PackedWord() = default;
    explicit PackedWord(const TInput data) : _data(data) {}

    template <typename TOut>
    static TOut ExtractBits(
        const TInput data,
        const UnsignedTinyInt shift,
        const TInput mask
    ){
        return static_cast<TOut>((data >> shift) & mask);
    }

    template<typename TOut, UnsignedTinyInt Shift, TInput Mask>
    static constexpr TOut ExtractBits(const TInput data){
        return static_cast<TOut>((data >> Shift) & Mask);
    }

    static void SetBits(
        TInput* data,
        const TInput value,
        const UnsignedTinyInt shift,
        const TInput mask
    ) {
        *data = static_cast<TInput>((*data & ~(mask << shift)) | ((value & mask) << shift));
    }

    template<UnsignedTinyInt Shift, TInput Mask>
    static constexpr void SetBits(
        TInput* data,
        const TInput value
    ){
        *data = static_cast<TInput>((*data & ~(Mask << Shift)) | ((value & Mask) << Shift));
    }

    static void SetBit(
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
    static constexpr void SetBit(
        TInput* data,
        const bool value
    ){
        if (value)
            *data |= static_cast<TInput>(TInput{1} << BitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << BitPosition));
    }

    template<UnsignedTinyInt BitPosition, bool Value>
    static constexpr void SetBit(
        TInput* data
    ){
        if constexpr (Value)
            *data |= static_cast<TInput>(TInput{1} << BitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << BitPosition));
    }


    template<bool Value>
    static constexpr void SetBit(
        TInput* data,
        const TInput bitPosition
    ){
        if constexpr (Value)
            *data |= static_cast<TInput>(TInput{1} << bitPosition);
        else
            *data &= static_cast<TInput>(~(TInput{1} << bitPosition));
    }

    static bool GetBit(
        const TInput data,
        const UnsignedTinyInt bitPosition
    ) {
        return (data & (TInput{1} << bitPosition)) != 0;
    }

    template<TInput BitPosition>
    static constexpr bool GetBit(
        const TInput data
    ){
        return (data & (TInput{1} << BitPosition)) != 0;
    }

    static constexpr UnsignedTinyInt SIZE = sizeof(TInput);
};

using PackedByte = PackedWord<UnsignedTinyInt>;