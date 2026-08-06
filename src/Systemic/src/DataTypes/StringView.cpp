#include "../../include/DataTypes/StringView.h"

#include <ostream>

#include "DataTypes/String.h"

namespace DataTypes{
    StringView::StringView(const char* data, const Int size){
        this->_data = data;
        this->_size = size;
    }

    StringView::StringView(const std::string& other)
        : _data(other.data()), _size(static_cast<Int>(other.size())){}

    StringView& StringView::operator=(StringView&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->_size = other._size;
        other._data = nullptr;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os, const StringView& sv){
        os.write(sv._data, sv._size);
        return os;
    }

    StringView::operator std::string_view() const{
        return std::string_view(this->_data, this->_size);
    }

    StringView::operator std::span<const char>() const{
        return std::span(this->_data, this->_size);
    }

    StringView StringView::Substring(const Int startIndex, const Int length) const{
        if (startIndex < 0 || startIndex >= this->_size)
            throw std::out_of_range("Index out of range.");

        if (length < 0 || startIndex + length > this->_size)
            throw std::out_of_range("Length out of range.");

        return StringView(this->_data + startIndex, length);
    }

    Int StringView::IndexOf(const char c) const{
        for (Int i = 0; i < this->_size; i++)
            if (this->_data[i] == c)
                return i;
        return -1;
    }

    bool StringView::Compare(const StringView& other, const StringComparisonType type) const{
        switch (type) {
        case StringComparisonType::Equals:
            return this->Equals(*this, other);
        case StringComparisonType::EqualsIgnoreCase:
            return this->EqualsIgnoreCase(*this, other);
        case StringComparisonType::StartsWith:
            return this->StartsWith(*this, other);
        case StringComparisonType::StartsWithIgnoreCase:
            return this->StartsWithIgnoreCase(*this, other);
        case StringComparisonType::EndsWith:
            return this->EndsWith(*this, other);
        case StringComparisonType::EndsWithIgnoreCase:
            return this->EndsWithIgnoreCase(*this, other);
        case StringComparisonType::Contains:
            return this->Contains(*this, other);
        case StringComparisonType::ContainsIgnoreCase:
            return this->ContainsIgnoreCase(*this, other);
        default:
            throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }
}
