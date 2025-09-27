#pragma once
#include <cstdint>
#include <string>
#include <utility>

namespace AdditionalDataTypes {
  enum ResultCode : uint8_t {
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
    Overflow = 12
  };

  struct ResultStatus {
    ResultCode code;
    std::string message;
    int64_t primaryKeyVal;
    
    ResultStatus() {
      this->code = ResultCode::Ok;
      this->primaryKeyVal = 0;
    }

    ResultStatus(const ResultCode& code, const std::string&  message)
      : code(code), message(message), primaryKeyVal(0) {}

  };
}

