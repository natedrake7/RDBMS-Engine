#pragma once

#include <string>
#include <limits>
#include <stdexcept>
#include <charconv>
#include <cstring>

#include "Constants.h"
#include "DataTypes/Decimal.h"
#include "DataTypes/String.h"
#include "DataTypes/StringValue.h"
#include "DataTypes/StringView.h"

class Converter {
    template<DataTypes::Primitive T>
    static T StrToInt(const char* data, const Int size){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        T value{};
        const auto [ptr, ec] = std::from_chars(data, data + size, value);

        // ec != {}  -> no digits or out of T's range;  ptr != end -> trailing garbage
        if (ec != std::errc{} || ptr != data + size)
            throw std::out_of_range("Converter::StrToInt: invalid or out-of-range integer string");

        return value;
    }

    template<DataTypes::Primitive T>
    static bool TryStrToInt(const char* data, const Int size){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        T value{};
        const auto [ptr, ec] = std::from_chars(data, data + size, value);

        return ec == std::errc{} && ptr == data + size;
    }

public:
    template<DataTypes::Primitive INT_TYPE, DataTypes::IsStringLike STR_TYPE>
    static INT_TYPE StrToInt(const STR_TYPE& input){
        if constexpr (
            std::is_same_v<STR_TYPE, DataTypes::StringView>
            || std::is_same_v<STR_TYPE, DataTypes::String>
            || std::is_same_v<STR_TYPE, DataTypes::StringValue>
        ){
            return StrToInt<INT_TYPE>(input.Data(), input.Size());
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, std::string>
            || std::is_same_v<STR_TYPE, std::string_view>
        ){
            return StrToInt<INT_TYPE>(input.data(), static_cast<Int>(input.size()));
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, const char*>
            || std::is_same_v<STR_TYPE, char*>
        ){
            return StrToInt<INT_TYPE>(input, static_cast<Int>(std::strlen(input)));
        }
        else static_assert(DataTypes::AlwaysFalse<STR_TYPE>, "Invalid type for StrToInt");
    }

    template<DataTypes::Primitive INT_TYPE, DataTypes::IsStringLike STR_TYPE>
    static bool TryStrToInt(const STR_TYPE& input) {
        if constexpr (
            std::is_same_v<STR_TYPE, DataTypes::StringView>
            || std::is_same_v<STR_TYPE, DataTypes::String>
        ){
            return TryStrToInt<INT_TYPE>(input.Data(), input.Size());
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, std::string>
            || std::is_same_v<STR_TYPE, std::string_view>
        ){
            return TryStrToInt<INT_TYPE>(input.data(), static_cast<Int>(input.size()));
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, const char*>
            || std::is_same_v<STR_TYPE, char*>
        ){
            return TryStrToInt<INT_TYPE>(input, static_cast<Int>(std::strlen(input)));
        }
        else static_assert(DataTypes::AlwaysFalse<STR_TYPE>, "Invalid type for StrToInt");
    }

    template<DataTypes::Primitive T>
    static bool TryDownCast(const BigInt input){
        static_assert(std::is_integral_v<T>, "T must be integral type");
        return !(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max());
    }

    template<DataTypes::Primitive T>
    static bool TryDownCast(const BigInt input, T& output){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (!(input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max())) {
            output = static_cast<T>(input);
            return true;
        }

        return false;
    }

    template<DataTypes::Primitive T>
    static T DownCast(const BigInt input){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        if (input < std::numeric_limits<T>::min() || input > std::numeric_limits<T>::max())
            throw std::out_of_range("Converter::DownCast: Value is out of range of the target type.");

        return static_cast<T>(input);
    }

    static DataTypes::Decimal Stod(const DataTypes::StringView& input) {
        return DataTypes::Decimal(input);
    }

    // True when the Decimal's encoded payload fits within `size` bytes (column width check).
    static bool DecimalFitsInSize(
        const DataTypes::Decimal &input,
        const UnsignedInt size
    ){
        return input.RawSize() <= size;
    }

    // static bool AssertOverflow(const T leftValue, const T rightValue){
    //     static_assert(std::is_integral_v<T>, "T must be integral type");
    //
    //     return ((rightValue > 0 && leftValue > std::numeric_limits<T>::max() - rightValue) ||
    //         (rightValue < 0 && leftValue < std::numeric_limits<T>::min() - rightValue));
    // }


    template<DataTypes::Primitive T>
    static auto IntToStr(const T input, const ::Memory::IAllocator* allocator){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        char buffer[ITOS_BUFFER_SIZE]{};
        const auto len = std::snprintf(buffer, sizeof(buffer), "%lld", static_cast<BigInt>(input));

        return DataTypes::String(buffer, len, allocator);
    }

    template<DataTypes::Primitive T>
    static DataTypes::StringValue IntToStringValue(const T input, const ::Memory::IAllocator* allocator){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        char buffer[ITOS_BUFFER_SIZE]{};
        const auto len = std::snprintf(buffer, sizeof(buffer), "%lld", static_cast<BigInt>(input));

        return DataTypes::StringValue::Create(allocator, buffer, len);
    }

    static DataTypes::String DecimalToStr(const DataTypes::Decimal& input, const ::Memory::IAllocator* allocator){
        return input.ToString(allocator);
    }

};