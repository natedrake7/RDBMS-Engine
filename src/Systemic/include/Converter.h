#pragma once

#include <string>
#include <limits>
#include <stdexcept>
#include "DataTypes/Decimal.h"
#include "DataTypes/String.h"

template<typename T>
class Converter {
public:
    static T Stoi(const std::string& input)
    {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (sizeof(T) > sizeof(int))
        {
            auto value = stoll(input);

            if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max())
                throw std::out_of_range("SafeStoi: Value is out of range of the target type.");

            return static_cast<T>(value);
        }

        int value = stoi(input);

        if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max())
            throw std::out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(value);
    }

    static DataTypes::Decimal Stod(const std::string& input) {
        return DataTypes::Decimal(input);
    }

    static bool TryStoi(const DataTypes::String& input) {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        const auto* str = input.GetDataAsChar();
        char* endptr = nullptr;
        errno = 0;  // Reset errno before the conversion

        long long value = strtoll(str, &endptr, 10);

        // Check for conversion errors
        if (endptr == str || *endptr != '\0' || errno == ERANGE)
            return false;

        // Check if the value is within the target type's range
        return !(value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max());
    }

    static T Stoi(const std::u16string& input)
    {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        const std::wstring converted(input.begin(), input.end());

        if (sizeof(T) > sizeof(int))
        {
            long long value = stoll(converted);

            if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max())
                throw std::out_of_range("SafeStoi: Value is out of range of the target type.");

            return static_cast<T>(value);
        }

        int value = stoi(converted);

        if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max())
            throw std::out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(value);
    }

    static bool TryStoi(const std::u16string& input) {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        const std::wstring converted(input.begin(), input.end());

        char* endPtr = nullptr;
        errno = 0;

        if (sizeof(T) > sizeof(int))
        {
            // strtoll(input.c_str(), &endPtr, 10);

            return errno != ERANGE;
        }

        // strtol(input.c_str(), &endPtr, 10);

        return errno != ERANGE;
    }
    
    static bool TryStoi(const BigInt input){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        return !(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max());
    }

    static bool TryStoi(const BigInt input, T& output){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (!(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max())) {
            output = static_cast<T>(input);
            return true;
        }

        return false;
    }

    static T Stoi(const BigInt input){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max())
            throw std::out_of_range("SafeStoi: Value is out of range of the target type.");

        return static_cast<T>(input);
    }

    static bool TryStoi(
        const DataTypes::Decimal &input,
        const UnsignedInt size
    ){

        // if (input > std::numeric_limits<DataTypes::Decimal>::max()
        //     || input < std::numeric_limits<DataTypes::Decimal>::min())
        //     return false;

        return input.GetRawDataSize() <= size;
    }

    static bool AssertOverflow(const T leftValue, const T rightValue)
    {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        return ((rightValue > 0 && leftValue > std::numeric_limits<T>::max() - rightValue) ||
            (rightValue < 0 && leftValue < std::numeric_limits<T>::min() - rightValue));
    }
};