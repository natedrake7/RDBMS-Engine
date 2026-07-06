#pragma once
#include <cstdint>
#include "Indexing/Key.h"

namespace Errors {
    enum class RuntimeError : uint8_t {
        Ok = 0,
        Error = 1,
        NotFound = 2,
        NotUnique = 3,
        NotSupported = 4,
        InvalidOperation = 5,
        InvalidArgument = 6,
        InvalidDataType = 7,
        InvalidTable = 8,
        InvalidColumn = 9,
        DuplicateKey = 10,
        ColumnSizeExceeded = 11,
        Overflow = 12,
        InvalidSession = 13,
    };

    struct RuntimeStatus {
        DataTypes::String message;

        DataTypes::Indexing::Key primaryKey;
        CoreEngine::StorageTypes::RID rid;

        RuntimeError code;

        explicit RuntimeStatus(const ::Memory::IAllocator* allocator)
            :   message(DataTypes::String::Empty(allocator)), primaryKey(DataTypes::Indexing::Key()),
                code(RuntimeError::Ok){}

        RuntimeStatus(const RuntimeError code, const DataTypes::String& message)
            : message(message), primaryKey(DataTypes::Indexing::Key()), code(code){}

        RuntimeStatus()
            :   message(DataTypes::String::Null()),
                primaryKey(DataTypes::Indexing::Key()),
                code(RuntimeError::Ok){}

        RuntimeStatus(
            const RuntimeError code,
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        ):  message(DataTypes::String(message, allocator)),
            primaryKey(DataTypes::Indexing::Key()),
            code(code){}

        RuntimeStatus(const RuntimeError code, DataTypes::String& message)
            : message(std::move(message)), primaryKey(DataTypes::Indexing::Key()), code(code){}

        RuntimeStatus(const RuntimeError code, DataTypes::String&& message)
            : message(std::move(message)), primaryKey(DataTypes::Indexing::Key()), code(code){}

        RuntimeStatus(RuntimeStatus&& other) noexcept
            : message(std::move(other.message)), primaryKey(std::move(other.primaryKey)), code(other.code){}

        RuntimeStatus& operator=(RuntimeStatus&& other) noexcept{
            if (this == &other)
                return *this;

            this->code = other.code;
            this->message = std::move(other.message);
            this->primaryKey = std::move(other.primaryKey);

            return *this;
        }

        [[nodiscard]] bool IsOk()const { return this->code == RuntimeError::Ok;}
    };

    enum class ValidationError : uint8_t {
        Ok = 0,
        Error = 1
    };

    struct ValidationStatus {
        ValidationError code;
        DataTypes::String message;

        ValidationStatus() {
            this->code = ValidationError::Ok;
        }

        explicit ValidationStatus(const ::Memory::IAllocator* allocator)
            : code(ValidationError::Ok), message(DataTypes::String::Empty(allocator)){}

        ValidationStatus(const ValidationError code, const DataTypes::String& message)
            : code(code), message(message){}

        ValidationStatus(const ValidationError code, DataTypes::String&& message) noexcept
            : code(code), message(std::move(message)){}

        ValidationStatus(
            const ValidationError code,
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        )   : code(code), message(DataTypes::String(message, allocator)){}

        static ValidationStatus Error(
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        ){
            return ValidationStatus(ValidationError::Error, message, allocator);
        }

        static ValidationStatus Error(DataTypes::String&& message){
            return ValidationStatus(ValidationError::Error, std::move(message));
        }

        static ValidationStatus Ok(){
            return ValidationStatus(ValidationError::Ok, DataTypes::String::Null());
        }

        [[nodiscard]] bool IsOk()const { return this->code == ValidationError::Ok; }
    };

    inline ValidationStatus operator&&(ValidationStatus lhs, ValidationStatus rhs) {
        if (!lhs.IsOk()) return lhs;
        if (!rhs.IsOk()) return rhs;
        return lhs;
    }

    struct Error {
        DataTypes::String message;
        bool hasError;

        explicit Error()
            : message(DataTypes::String::Null()), hasError(false) {}

        explicit Error(const ::Memory::IAllocator* allocator)
            : message(DataTypes::String::Empty(allocator)), hasError(false) {}

        Error(const bool hasError, const DataTypes::String& message)
            : message(message), hasError(hasError) {}

        Error(const bool hasError, DataTypes::String&& message) noexcept
            : message(std::move(message)), hasError(hasError) {}

        Error(const bool hasError, const DataTypes::StringView& message, const ::Memory::IAllocator* allocator)
            : message(DataTypes::String(message, allocator)), hasError(hasError) {}
    };
}
