#pragma once
#include <cstdint>

#include "Key.h"
#include "RowIdentifier.h"

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
    RuntimeError code;
    DataTypes::String message;

    DataTypes::Indexing::Key primaryKey;
    DataTypes::RowIdentifier rowId;

    explicit RuntimeStatus(const ::Memory::IAllocator* allocator) {
      this->code = RuntimeError::Ok;
      this->message = DataTypes::String::Empty(allocator);
      this->primaryKey = DataTypes::Indexing::Key();
    }

    // RuntimeStatus(const RuntimeError code, DataTypes::String& message){
    //     this->code = code;
    //     this->message = std::move(message);
    //     this->primaryKey = DataTypes::Indexing::Key();
    // }
    //
    // RuntimeStatus(const RuntimeError code, const DataTypes::String_view message){
    //     this->code = code;
    //     this->message = DataTypes::String(message);
    //     this->primaryKey = DataTypes::Indexing::Key();
    // }
    //
    // RuntimeStatus(const RuntimeError code, const DataTypes::String&  message){
    //   this->code = code;
    //   this->message = message;
    //   this->primaryKey = DataTypes::Indexing::Key();
    // }

    RuntimeStatus(){
        this->code = RuntimeError::Ok;
    }

    RuntimeStatus(const RuntimeError code, const DataTypes::StringView& message, const ::Memory::IAllocator* allocator){
        this->code = code;
        this->message = DataTypes::String(message, allocator);
        this->primaryKey = DataTypes::Indexing::Key();
    }

    RuntimeStatus(const RuntimeError code, DataTypes::String& message){
        this->code = code;
        this->message = std::move(message);
        this->primaryKey = DataTypes::Indexing::Key();
    }

    RuntimeStatus(const RuntimeError code, DataTypes::String&& message){
        this->code = code;
        this->message = std::move(message);
        this->primaryKey = DataTypes::Indexing::Key();
    }

    RuntimeStatus(RuntimeStatus&& other) noexcept {
      this->code = other.code;
      this->message = std::move(other.message);
      this->primaryKey = std::move(other.primaryKey);
      this->rowId = other.rowId;
    }

    RuntimeStatus& operator=(RuntimeStatus&& other) noexcept{
      if (this == &other)
        return *this;

      this->code = other.code;
      this->message = std::move(other.message);
      this->primaryKey = std::move(other.primaryKey);
      this->rowId = other.rowId;

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

    explicit ValidationStatus(const ::Memory::IAllocator* allocator) {
      this->code = ValidationError::Ok;
      this->message = DataTypes::String::Empty(allocator);
    }

    ValidationStatus(const ValidationError code, const DataTypes::String& message){
      this->code = code;
      this->message = message;
    }

    ValidationStatus(const ValidationError code, DataTypes::String&& message) noexcept {
      this->code = code;
      this->message = std::move(message);
    }

    ValidationStatus(
        const ValidationError code,
        const DataTypes::StringView& message,
        const ::Memory::IAllocator* allocator
    ){
        this->code = code;
        this->message = DataTypes::String(message, allocator);
    }

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

  inline ValidationStatus operator&&(const ValidationStatus& lhs, const ValidationStatus& rhs) {
    if (!lhs.IsOk())
      return lhs;

    if (!rhs.IsOk())
      return rhs;

    return lhs; // or rhs, since both are OK
  }

  struct Error {
    bool hasError;
    DataTypes::String message;

    explicit Error(const ::Memory::IAllocator* allocator) {
      this->hasError = false;
      this->message = DataTypes::String::Empty(allocator);
    }

    Error(const bool hasError, const DataTypes::String& message){
      this->hasError = hasError;
      this->message = message;
    }
  };
}

