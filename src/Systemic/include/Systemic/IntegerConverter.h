#pragma once
#include <string>
#include <limits>
#include <stdexcept>
#include <charconv>
#include <cstring>

#include <Systemic/Constants.h>
#include <Systemic/DataTypes/Decimal.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/DataTypes/StringValue.h>
#include <Systemic/DataTypes/StringView.h>

class IntegerConverter {
    template<typename T>
    inline static constexpr size_t ITOS_BUFFER_SIZE =
        std::numeric_limits<T>::digits10 + 4;

    template<DataTypes::Primitive T>
    static T StrToInt(const char* data, const Int32_t size){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        T value{};
        const auto [ptr, ec] = std::from_chars(data, data + size, value);

        // ec != {}  -> no digits or out of T's range;  ptr != end -> trailing garbage
        if (ec != std::errc{} || ptr != data + size)
            throw std::out_of_range("Converter::StrToInt: invalid or out-of-range integer string");

        return value;
    }

    template<DataTypes::Primitive T>
    static bool TryStrToInt(const char* data, const Int32_t size){
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
            return StrToInt<INT_TYPE>(input.data(), static_cast<Int32_t>(input.size()));
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, const char*>
            || std::is_same_v<STR_TYPE, char*>
        ){
            return StrToInt<INT_TYPE>(input, static_cast<Int32_t>(std::strlen(input)));
        }
        else static_assert(DataTypes::AlwaysFalse<STR_TYPE>, "Invalid type for StrToInt");

        std::unreachable();
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
            return TryStrToInt<INT_TYPE>(input.data(), static_cast<Int32_t>(input.size()));
        }
        else if constexpr (
            std::is_same_v<STR_TYPE, const char*>
            || std::is_same_v<STR_TYPE, char*>
        ){
            return TryStrToInt<INT_TYPE>(input, static_cast<Int32_t>(std::strlen(input)));
        }
        else static_assert(DataTypes::AlwaysFalse<STR_TYPE>, "Invalid type for StrToInt");

        std::unreachable();
    }

    template<DataTypes::IsInteger TInput, DataTypes::IsInteger TOutput>
    inline static constexpr bool CanDownCast(const TInput input){
        return !(
            input < std::numeric_limits<TOutput>::min()
            || input > std::numeric_limits<TOutput>::max()
        );
    }

    template<DataTypes::IsInteger TInput, DataTypes::IsInteger TOutput>
    inline static constexpr bool TryDownCast(const TInput input, TOutput& output){
        if (!(input < std::numeric_limits<TOutput>::min() || input > std::numeric_limits<TOutput>::max())) {
            output = static_cast<TOutput>(input);
            return true;
        }

        return false;
    }

    template<DataTypes::Primitive TInput, DataTypes::Primitive TOutput>
    inline static constexpr TOutput DownCast(const TInput input){
        assert(
            input < std::numeric_limits<TOutput>::min() && input > std::numeric_limits<TOutput>::max()
            && "Converter::DownCast: Value is out of range of the target type."
        );
        return static_cast<TOutput>(input);
    }

    template<DataTypes::Primitive T>
    static auto IntToStr(const T input, const ::Memory::IAllocator* allocator){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        char buffer[ITOS_BUFFER_SIZE<T>];

        const auto [end, ec] = std::to_chars(
            buffer,
            buffer + sizeof(buffer),
            input
        );

        if (ec != std::errc{}) [[unlikely]]
            throw std::runtime_error("Integer conversion failed");

        return DataTypes::String(static_cast<const char*>(buffer), static_cast<data_size_t>(end - buffer), allocator);
    }

    template<DataTypes::Primitive T>
    static DataTypes::StringValue IntToStringValue(const T input, const ::Memory::IAllocator* allocator){
        static_assert(std::is_integral_v<T>, "T must be integral type");

        char buffer[ITOS_BUFFER_SIZE<T>];

        const auto [end, ec] = std::to_chars(
            buffer,
            buffer + sizeof(buffer),
            input
        );

        if (ec != std::errc{}) [[unlikely]]
            throw std::runtime_error("Integer conversion failed");

        return DataTypes::StringValue::Create(allocator, buffer, static_cast<data_size_t>(end - buffer));
    }
};