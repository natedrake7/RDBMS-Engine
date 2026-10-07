#pragma once
#include <cstdint>
#include <CoreEngine/Indexing/Key.h>
#include <CoreEngine/DataStorage/Row/Row.h>

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

        explicit RuntimeStatus(const RuntimeError code, const DataTypes::String& message)
            : message(message), primaryKey(DataTypes::Indexing::Key()), code(code){}

        explicit RuntimeStatus()
            :   message(DataTypes::String::Null()),
                primaryKey(DataTypes::Indexing::Key()),
                code(RuntimeError::Ok){}

        explicit RuntimeStatus(
            const RuntimeError code,
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        ):  message(DataTypes::String(message, allocator)),
            primaryKey(DataTypes::Indexing::Key()),
            code(code){}

        explicit RuntimeStatus(const RuntimeError code, DataTypes::String& message)
            : message(std::move(message)), primaryKey(DataTypes::Indexing::Key()), code(code){}

        explicit RuntimeStatus(const RuntimeError code, DataTypes::String&& message)
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

    enum class CompilationError : uint8_t {
        Ok = 0,
        Error = 1
    };

    struct CompilationStatus {
        DataTypes::String _message;
        CompilationError _code;

        explicit CompilationStatus()
            :   _code(CompilationError::Ok){}

        explicit CompilationStatus(const ::Memory::IAllocator* allocator)
            : _message(DataTypes::String::Empty(allocator)), _code(CompilationError::Ok){}

        explicit CompilationStatus(const CompilationError code, const DataTypes::String& message)
            : _message(message), _code(code){}

        explicit CompilationStatus(const CompilationError code, DataTypes::String&& message) noexcept
            : _message(std::move(message)), _code(code){}

        explicit CompilationStatus(
            const CompilationError code,
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        )   : _message(DataTypes::String(message, allocator)), _code(code){}

        inline static CompilationStatus Error(
            const DataTypes::StringView& message,
            const ::Memory::IAllocator* allocator
        ){
            return CompilationStatus(CompilationError::Error, message, allocator);
        }

        inline static CompilationStatus Error(DataTypes::String&& message){
            return CompilationStatus(CompilationError::Error, std::move(message));
        }

        inline static CompilationStatus Ok(){
            return CompilationStatus(CompilationError::Ok, DataTypes::String::Null());
        }

        [[nodiscard]] inline bool IsOk()const { return this->_code == CompilationError::Ok; }
    };

    inline CompilationStatus operator&&(CompilationStatus lhs, CompilationStatus rhs) {
        if (!lhs.IsOk()) return lhs;
        if (!rhs.IsOk()) return rhs;
        return lhs;
    }

    struct ParserStatus {
        DataTypes::String message;
        bool hasError;

        explicit ParserStatus()
            : message(DataTypes::String::Null()), hasError(false) {}

        explicit ParserStatus(const ::Memory::IAllocator* allocator)
            : message(DataTypes::String::Empty(allocator)), hasError(false) {}

        explicit ParserStatus(const bool hasError, const DataTypes::String& message)
            : message(message), hasError(hasError) {}

        explicit ParserStatus(const bool hasError, DataTypes::String&& message) noexcept
            : message(std::move(message)), hasError(hasError) {}

        explicit ParserStatus(const bool hasError, const DataTypes::StringView& message, const ::Memory::IAllocator* allocator)
            : message(DataTypes::String(message, allocator)), hasError(hasError) {}
    };
}
