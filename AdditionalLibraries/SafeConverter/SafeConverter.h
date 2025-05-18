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
    
    static T SafeStoi(const int64_t &input){
        static_assert(is_integral<T>::value, "T must be integral type");

        if (input < numeric_limits<T>::min() || input > numeric_limits<T>::max())
            throw out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(input);
    }
};