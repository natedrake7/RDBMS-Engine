﻿#include "SafeConverter.h"

template<typename T>
T SafeConverter<T>::SafeStoi(const string& input)
{
    static_assert(is_integral<T>::value, "T must be integral type");

    if (sizeof(T) > sizeof(int))
    {
        long long value = stoll(input);

        if (value < numeric_limits<T>::min() || value > numeric_limits<T>::max())
            throw out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(value);
    }

    int value = stoi(input);

    if (value < numeric_limits<T>::min() || value > numeric_limits<T>::max())
        throw out_of_range("SafeStoi: Value is out of range of the target type.");

    return static_cast<T>(value);
}

template int8_t SafeConverter<int8_t>::SafeStoi(const string& input);
template int16_t SafeConverter<int16_t>::SafeStoi(const string& input);
template int32_t SafeConverter<int32_t>::SafeStoi(const string& input);
template int64_t SafeConverter<int64_t>::SafeStoi(const string& input);
