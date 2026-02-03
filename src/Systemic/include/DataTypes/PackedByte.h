#pragma once
#include "DataTypes.h"

class PackedByte {
public:
    UnsignedTinyInt _data;

    PackedByte() : _data(0) {}
    explicit PackedByte(const UnsignedTinyInt value) : _data(value) {}

    template <typename T>
    static T ExtractBits(
        const UnsignedTinyInt data,
        const UnsignedTinyInt shift,
        const UnsignedTinyInt mask
    ){
        return static_cast<T>((data >> shift) & mask);
    }

    static void SetBits(
        UnsignedTinyInt& data,
        const UnsignedTinyInt value,
        const UnsignedTinyInt shift,
        const UnsignedTinyInt mask
    ) {
        data = (data & ~(mask << shift)) | ((value & mask) << shift);
    }

    static void SetBit(UnsignedTinyInt& data, const UnsignedTinyInt bitPosition, const bool value) {
        if (value)
            data |= (1 << bitPosition);
        else
            data &= ~(1 << bitPosition);
    }

    static bool GetBit(const UnsignedTinyInt data, const UnsignedTinyInt bitPosition) {
        return (data & (1 << bitPosition)) != 0;
    }

    static constexpr UnsignedTinyInt Size = sizeof(UnsignedTinyInt);
};