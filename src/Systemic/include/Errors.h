#pragma once
#include <cstdint>
#include <string>

#include "Headers.h"
#include "Key.h"

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
    std::string message;

    DataTypes::Indexing::Key primaryKey;
    Headers::RowIdentifier rowId;

    RuntimeStatus() {
      this->code = RuntimeError::Ok;
      this->message = "";
      this->primaryKey = DataTypes::Indexing::Key();
    }

    RuntimeStatus(const RuntimeError& code, const std::string&  message){
      this->code = code;
      this->message = message;
      this->primaryKey = DataTypes::Indexing::Key();
    }
  };

  enum class ValidationError : uint8_t {
    Ok = 0,
    Error = 1
  };

  struct ValidationStatus {
    ValidationError code;
    std::string message;

    ValidationStatus() {
      this->code = ValidationError::Ok;
      this->message = "";
    }

    ValidationStatus(const ValidationError& code, const std::string&  message){
      this->code = code;
      this->message = message;
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
    std::string message;

    Error() {
      this->hasError = false;
      this->message = "";
    }

    Error(const bool& hasError, const std::string&  message){
      this->hasError = hasError;
      this->message = message;
    }
  };
}

