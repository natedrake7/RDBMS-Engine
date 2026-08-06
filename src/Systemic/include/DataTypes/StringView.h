#pragma once
#include <span>
#include "DataTypes.h"
#include <ostream>
#include "StringValue.h"

namespace Memory{
    class IAllocator;
}

namespace DataTypes{
    class String;

    class StringView{
        const char* _data;
        Int _size;

        template <bool IgnoreCase>
        [[nodiscard]] static constexpr bool MatchesAt(
            const char* haystack,
            const char* needle,
            const Int count
        ) noexcept{
                    if !consteval{
                        if constexpr (!IgnoreCase)
                            return std::memcmp(haystack, needle, count) == 0;
                    }

                    for (Int i = 0; i < count; i++) {
                        if (IgnoreCase) {
                            if (Lower(haystack[i]) != Lower(needle[i]))
                                return false;
                        }
                        else {
                            if (haystack[i] != needle[i])
                                return false;
                        }
                    }

                    return true;
                }

        template <bool IgnoreCase>
        [[nodiscard]] static constexpr bool ContainsImplementation(const StringView& lhs, const StringView& rhs) noexcept{
            if (rhs._size == 0)
                return true;
            if (lhs._size < rhs._size)
                return false;

            for (Int i = 0; i <= lhs._size - rhs._size; i++)
                if (MatchesAt<IgnoreCase>(lhs._data + i, rhs._data, rhs._size)) return true;

            return false;
        }

        public:
        /**
             *
             * @param data Non-owning pointer of the actual data
             * @param size The size of the string view in bytes (not including null terminator, if any).
             * The string view can contain null characters within it and is not required to be null-terminated.
        */
        StringView(const char* data, Int size);

        constexpr StringView(const StringView& other)noexcept{
            this->_data = other._data;
            this->_size = other._size;
        }

        constexpr StringView(StringView&& other) noexcept
            : _data(other._data), _size(other._size){
                other._data = nullptr;
        }

        constexpr StringView()
            : _data(nullptr), _size(0){}

        constexpr StringView(const char* other)
            : _data(other), _size(StringView::CalculateSize(other)){}

        constexpr StringView& operator=(const char* other){
            this->_data = other;
            this->_size = StringView::CalculateSize(other);
            return *this;
        }

        explicit StringView(const std::string& other);

        StringView& operator=(StringView&& other) noexcept;
        constexpr StringView& operator=(const StringView& other)= default;
        constexpr ~StringView() = default;

        [[nodiscard]] constexpr const char* Data() const noexcept{ return this->_data; }

        //operators
        friend std::ostream& operator<<(std::ostream& os, const StringView& sv);
        [[nodiscard]] constexpr char operator[](const Int index) const{
            return this->_data[index];
        }

        //Equality Operators
        [[nodiscard]] constexpr bool operator==(const char* other) const{
            return this->Compare<StringComparisonType::Equals, const char*>(other);
        }

        [[nodiscard]] constexpr bool operator==(const StringView& other) const{
            return this->Compare<StringComparisonType::Equals, StringView>(other);
        }

        [[nodiscard]] constexpr bool operator!=(const char* other) const{
            return !(*this == other);
        }

        [[nodiscard]] constexpr bool operator!=(const StringView& other) const{
            return !(*this == other);
        }

        //Comparison Operators
        [[nodiscard]] constexpr bool operator<(const char* other) const {
            const auto otherSize = StringView::CalculateSize(other);

            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other, size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<(const StringView& other) const noexcept{
            const auto size = std::min(this->_size, other._size);
            const auto cmp = std::memcmp(this->_data, other._data, size);
            if (cmp != 0) return cmp < 0;

            return this->_size < other._size;
        }

        [[nodiscard]] constexpr bool operator<(const std::string& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<(const std::string_view& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<=(const char* other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const StringView& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const std::string& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const std::string_view& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator>(const StringView& other) const noexcept{
            const auto size = std::min(this->_size, other._size);
            const auto cmp = std::memcmp(this->_data, other._data, size);
            if (cmp != 0) return cmp > 0;

            return this->_size > other._size;
        }

        [[nodiscard]] constexpr bool operator>(const char* other) const {
            const auto otherSize = StringView::CalculateSize(other);

            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other, size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>(const std::string& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>(const std::string_view& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>=(const char* other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const StringView& other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const std::string& other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const std::string_view& other) const {
            return !(*this < other);
        }

        operator std::string_view() const;
        operator std::span<const char>() const;

        //functions
        [[nodiscard]] StringView Substring(Int startIndex, Int length) const;

        [[nodiscard]] constexpr Int Size()const{ return this->_size; }

        [[nodiscard]] Int IndexOf(char c) const;

        [[nodiscard]] bool Compare(const StringView& other, StringComparisonType type) const;
        template<IsStringLike TOther>
        [[nodiscard]] bool Compare(const TOther& other, const StringComparisonType type) const{
            return this->Compare(StringView::ViewOf(other), type);
        }

        template<StringComparisonType Type, IsStringLike TOther>
        [[nodiscard]] bool Compare(const TOther& other) const{
            const auto otherView = StringView::ViewOf(other);
            if constexpr (Type == StringComparisonType::Equals)
                return this->Equals(*this, otherView);
            else if constexpr (Type == StringComparisonType::EqualsIgnoreCase)
                return this->EqualsIgnoreCase(*this, otherView);
            else if constexpr (Type == StringComparisonType::StartsWith)
                return this->StartsWith(*this, otherView);
            else if constexpr (Type == StringComparisonType::StartsWithIgnoreCase)
                return this->StartsWithIgnoreCase(*this, otherView);
            else if constexpr (Type == StringComparisonType::EndsWith)
                return this->EndsWith(*this, otherView);
            else if constexpr (Type == StringComparisonType::EndsWithIgnoreCase)
                return this->EndsWithIgnoreCase(*this, otherView);
            else if constexpr (Type == StringComparisonType::Contains)
                return this->Contains(*this, otherView);
            else if constexpr (Type == StringComparisonType::ContainsIgnoreCase)
                return this->ContainsIgnoreCase(*this, otherView);
            else
                static_assert(AlwaysFalse<TOther>, "Compare: unsupported StringComparisonType");

            return false;
        }

        template <StringComparisonType Type, IsStringLike TLeft, IsStringLike TRight>
        static constexpr bool Compare(const TLeft& lhs, const TRight& rhs) {
            const auto leftView  = StringView::ViewOf(lhs);
            const auto rightView = StringView::ViewOf(rhs);
            return leftView.template Compare<Type>(rightView);
        }

        [[nodiscard]] constexpr bool Empty() const { return this->_size == 0; };

        // STL compatibility
        using const_iterator = const char*;

        [[nodiscard]] constexpr const_iterator begin() const{
            return this->_data;
        }
        
        [[nodiscard]] constexpr const_iterator end() const{
            return this->_data + this->_size;
        }

        static constexpr Int CalculateSize(const char* str){
            if (!str) return 0;

            Int size = 0;
            while (str[size] != '\0') size++;
            return size;
        }

        template <IsStringLike T>
        static StringView ViewOf(const T& str){
            if constexpr (
                std::is_same_v<std::decay_t<T>, StringView>
                || std::is_same_v<std::decay_t<T>, String>
                || std::is_same_v<std::decay_t<T>, StringValue>
            ){
                return StringView(str.Data(), str.Size());
            }

            else if constexpr (
                std::is_same_v<std::decay_t<T>, std::string>
                || std::is_same_v<std::decay_t<T>, std::string_view>
            ){
                return StringView(str.data(), static_cast<Int>(str.size()));
            }
            else if constexpr (std::is_same_v<std::decay_t<T>, char*> ||
                               std::is_same_v<std::decay_t<T>, const char*>
            ){
                return StringView(str, static_cast<Int>(std::strlen(str)));
            }
            else if constexpr (
                std::is_same_v<std::decay_t<T>, const char>
                || std::is_same_v<std::decay_t<T>, char>
            ){
                return StringView(&str, 1);
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "Invalid type for StringView constructor");

            return StringView();
        }

        static StringView ViewOf(const char* str, const Int size){
            return StringView(str, size);
        }

        [[nodiscard]] static constexpr char Lower(const char c) noexcept{
            return (c >= 'A' && c <= 'Z')
                ? (c + 'a' - 'A')
                : c;
        }

        [[nodiscard]] inline static constexpr bool Equals(const StringView& lhs, const StringView& rhs){
            return lhs._size == rhs._size
                && MatchesAt<false>(lhs._data, rhs._data, lhs._size);
        }

        [[nodiscard]] inline static constexpr bool EqualsIgnoreCase(const StringView& lhs, const StringView& rhs){
            return lhs._size == rhs._size
                && MatchesAt<true>(lhs._data, rhs._data, lhs._size);
        }

        [[nodiscard]] inline static constexpr bool StartsWith(const StringView& lhs, const StringView& rhs){
            return lhs._size >= rhs._size
                && MatchesAt<false>(lhs._data, rhs._data, rhs._size);
        }

        [[nodiscard]] inline static constexpr bool StartsWithIgnoreCase(const StringView& lhs, const StringView& rhs){
            return lhs._size >= rhs._size
                && MatchesAt<true>(lhs._data, rhs._data, rhs._size);
        }

        [[nodiscard]] inline static constexpr bool EndsWith(const StringView& lhs, const StringView& rhs){
            return lhs._size >= rhs._size
                && MatchesAt<false>(lhs._data + lhs._size - rhs._size, rhs._data, rhs._size);
        }

        [[nodiscard]] inline static constexpr bool EndsWithIgnoreCase(const StringView& lhs, const StringView& rhs){
            return lhs._size >= rhs._size
                && MatchesAt<true>(lhs._data + lhs._size - rhs._size, rhs._data, rhs._size);
        }

        [[nodiscard]] inline static constexpr bool Contains(const StringView& lhs, const StringView& rhs){
            return StringView::ContainsImplementation<false>(lhs, rhs);
        }

        [[nodiscard]] inline static constexpr bool ContainsIgnoreCase(const StringView& lhs, const StringView& rhs){
            return StringView::ContainsImplementation<true>(lhs, rhs);
        }
    };

    struct StringEqualsIgnoreCase {
        bool operator()(const StringValue& lhs, const StringValue& rhs) const{
            return StringView::Compare<StringComparisonType::Equals>(lhs, rhs);
        }
    };

    class StringViewStreamBuf final : public std::streambuf {
    public:
        explicit StringViewStreamBuf(const StringView& view) {
            auto* data = const_cast<char*>(view.Data());
            this->setg(data, data, data + view.Size());
        }
    };
}

template <>
struct std::hash<DataTypes::StringView> {
    size_t operator()(const DataTypes::StringView& str) const noexcept {
        // FNV-1a hash
        size_t hash = 14695981039346656037ULL;
        for (size_t i = 0; i < str.Size(); ++i) {
            hash ^= static_cast<size_t>(str.Data()[i]);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};
