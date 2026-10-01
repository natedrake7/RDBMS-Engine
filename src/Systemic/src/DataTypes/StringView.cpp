#include "../../include/DataTypes/StringView.h"

#include <ostream>

#include "DataTypes/String.h"

namespace DataTypes{
    StringView::StringView(const std::string& other)
        : _data(other.data()), _size(static_cast<data_size_t>(other.size())){}

    std::ostream& operator<<(std::ostream& os, const StringView& sv){
        os.write(sv._data, sv._size);
        return os;
    }

    StringView::operator std::string_view() const{
        return {this->_data, static_cast<std::size_t>(this->_size)};
    }

    StringView::operator std::span<const char>() const{
        return {this->_data, static_cast<std::size_t>(this->_size)};
    }

    StringView StringView::Substring(const data_size_t startIndex, const data_size_t length) const{
        if (startIndex < 0 || startIndex >= this->_size)
            throw std::out_of_range("Index out of range.");

        if (length < 0 || startIndex + length > this->_size)
            throw std::out_of_range("Length out of range.");

        return {this->_data + startIndex, length};
    }

    data_size_t StringView::IndexOf(const char c) const{
        for (data_size_t i = 0; i < this->_size; i++)
            if (this->_data[i] == c)
                return i;
        return -1;
    }

    bool StringView::Compare(const StringView& other, const StringComparisonType type) const{
        switch (type) {
        case StringComparisonType::Equals:
            return StringView::Equals(*this, other);
        case StringComparisonType::EqualsIgnoreCase:
            return StringView::EqualsIgnoreCase(*this, other);
        case StringComparisonType::StartsWith:
            return StringView::StartsWith(*this, other);
        case StringComparisonType::StartsWithIgnoreCase:
            return StringView::StartsWithIgnoreCase(*this, other);
        case StringComparisonType::EndsWith:
            return StringView::EndsWith(*this, other);
        case StringComparisonType::EndsWithIgnoreCase:
            return StringView::EndsWithIgnoreCase(*this, other);
        case StringComparisonType::Contains:
            return StringView::Contains(*this, other);
        case StringComparisonType::ContainsIgnoreCase:
            return StringView::ContainsIgnoreCase(*this, other);
        default:
            throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }
}
