#include "../../include/DataTypes/StringView.h"

#include <cstring>
#include <ostream>

#include "DataTypes/String.h"

namespace DataTypes{
    bool StringView::EqualsIgnoreCase(const char* other, const Int size) const{
        if (this->_size != size)
            return false;

        for (Int i = 0; i < this->_size; i++) {
            if (std::tolower(this->_data[i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool StringView::StartsWith(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        return std::memcmp(this->_data, other, size) == 0;
    }

    bool StringView::StartsWithIgnoreCase(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        for (int i = 0; i < size; i++){
            if (std::tolower(this->_data[i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool StringView::EndsWith(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        return std::memcmp(this->_data + this->_size - size, other, size) == 0;
    }

    bool StringView::EndsWithIgnoreCase(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        for (int i = 0; i < size; i++){
            if (std::tolower(this->_data[this->_size - size + i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool StringView::Contains(const char* other, const Int size) const {
        if (size == 0)
            return true;

        if (this->_size < size)
            return false;

        for (Int i = 0; i <= this->_size - size; i++) {
            if (std::memcmp(this->_data + i, other, size) == 0)
                return true;
        }

        return false;
    }

    bool StringView::ContainsIgnoreCase(const char* other, const Int size) const {
        if (size == 0)
            return true;

        if (this->_size < size)
            return false;

        for (Int i = 0; i <= this->_size - size; i++) {
            bool match = true;
            for (Int j = 0; j < size; j++) {
                if (std::tolower(this->_data[i + j]) != std::tolower(other[j])) {
                    match = false;
                    break;
                }
            }
            if (match)
                return true;
        }

        return false;
    }

    StringView::StringView(const char* data, const Int size){
        this->_data = data;
        this->_size = size;
    }

    StringView::StringView(StringView&& other) noexcept
        : _data(other._data), _size(other._size){
        other._data = nullptr;
    }

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

    bool StringView::Contains(const StringView& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return *this == other;
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other._data, other._size);
            case StringComparisonType::StartsWith:
                return this->StartsWith(other._data, other._size);
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other._data, other._size);
            case StringComparisonType::EndsWith:
                return this->EndsWith(other._data, other._size);
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other._data, other._size);
            case StringComparisonType::Contains:
                return this->Contains(other._data, other._size);
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other._data, other._size);
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool StringView::Contains(const String& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.Data(), other.Size());
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.Data(), other.Size());
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.Data(), other.Size());
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::Contains:
                return this->Contains(other.Data(), other.Size());
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.Data(), other.Size());
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool StringView::Contains(const char* other, const StringComparisonType type) const{
        const auto otherSize = static_cast<Int>(std::strlen(other));
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other, otherSize);
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other, otherSize);
            case StringComparisonType::StartsWith:
                return this->StartsWith(other, otherSize);
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other, otherSize);
            case StringComparisonType::EndsWith:
                return this->EndsWith(other, otherSize);
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other, otherSize);
            case StringComparisonType::Contains:
                return this->Contains(other, otherSize);
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other, otherSize);
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool StringView::Contains(const std::string_view other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::Contains:
                return this->Contains(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool StringView::Contains(const std::string& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::Contains:
                return this->Contains(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool StringView::Empty() const{ return this->_size == 0; }
}
