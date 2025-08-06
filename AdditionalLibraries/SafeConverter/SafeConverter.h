#pragma once

#include <string>
#include <limits>
#include <stdexcept>
#include <cstdint>

using namespace std;

template<typename T>
class SafeConverter {
public:
    static T SafeStoi(const string& input)
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

    static T SafeStoi(const u16string& input)
    {
        static_assert(is_integral<T>::value, "T must be integral type");

        wstring converted(input.begin(), input.end());

        if (sizeof(T) > sizeof(int))
        {
            long long value = stoll(converted);

            if (value < numeric_limits<T>::min() || value > numeric_limits<T>::max())
                throw out_of_range("SafeStoi: Value is out of range of the target type.");

            return static_cast<T>(value);
        }

        int value = stoi(converted);

        if (value < numeric_limits<T>::min() || value > numeric_limits<T>::max())
            throw out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(value);
    }
    
    static T SafeStoi(const int64_t &input){
        static_assert(is_integral<T>::value, "T must be integral type");

        if (input < numeric_limits<T>::min() || input > numeric_limits<T>::max())
            throw out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(input);
    }

    static bool AssertOverflow(const T& leftValue, const T& rightValue)
    {
        static_assert(is_integral<T>::value, "T must be integral type");

        return ((rightValue > 0 && leftValue > numeric_limits<T>::max() - rightValue) ||
            (rightValue < 0 && leftValue < numeric_limits<T>::min() - rightValue));
    }
};