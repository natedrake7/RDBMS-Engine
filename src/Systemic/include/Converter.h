#pragma once

#include <string>
#include <limits>
#include <stdexcept>

#include "Constants.h"
#include "DataTypes/Decimal.h"
#include "DataTypes/String.h"
#include "DataTypes/StringView.h"

template<typename T>
class Converter {
    static T Stoi(const char* data, const Int size){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        T value{};
        const auto [ptr, ec] = std::from_chars(data, data + size, value);

        // ec != {}  -> no digits or out of T's range;  ptr != end -> trailing garbage
        if (ec != std::errc{} || ptr != data + size)
            throw std::out_of_range("Converter::Stoi: invalid or out-of-range integer string");

        return value;
    }

    static T TryStoi(const char* data, const Int size){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        T value{};
        const auto [ptr, ec] = std::from_chars(data, data + size, value);

        return ec == std::errc{} && ptr == data + size;
    }

public:
    static T Stoi(const DataTypes::StringView& input){
        return Stoi(input.Data(), input.Size());
    }

    static T Stoi(const DataTypes::String& input){
        return Stoi(input.Data(), input.Size());
    }

    static T Stoi(const std::string& input){
        return Stoi(input.data(), static_cast<Int>(input.size()));
    }

    static DataTypes::Decimal Stod(const DataTypes::StringView& input) {
        return DataTypes::Decimal(input);
    }

    static bool TryStoi(const DataTypes::StringView& input) {
        return TryStoi(input.Data(), input.Size());
    }

    static bool TryStoi(const std::string& input) {
        return TryStoi(input.data(), static_cast<Int>(input.size()));
    }
    
    static bool TryDownCast(const BigInt input){
        static_assert(std::is_integral_v<T>, "T must be integral type");
        return !(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max());
    }

    static bool TryDownCast(const BigInt input, T& output){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (!(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max())) {
            output = static_cast<T>(input);
            return true;
        }

        return false;
    }

    static T DownCast(const BigInt input){
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

    static bool AssertOverflow(const T leftValue, const T rightValue){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        return ((rightValue > 0 && leftValue > std::numeric_limits<T>::max() - rightValue) ||
            (rightValue < 0 && leftValue < std::numeric_limits<T>::min() - rightValue));
    }


    static DataTypes::String Itos(const T input, const ::Memory::IAllocator* allocator){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        char buffer[ITOS_BUFFER_SIZE] = {};
        const auto len = std::snprintf(buffer, sizeof(buffer), "%lld", static_cast<long long>(input));

        return DataTypes::String(buffer, len, allocator);
    }

    static DataTypes::String Dtos(const DataTypes::Decimal& input, const ::Memory::IAllocator* allocator){
        return input.ToString(allocator);
    }

};