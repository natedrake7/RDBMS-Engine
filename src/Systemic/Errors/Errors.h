#pragma once
#include <cstdint>
#include <string>
#include "../Indexing/Key.h"

namespace Errors {
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
    DataTypes::Indexing::Key primaryKey;

    ResultStatus() {
      this->code = ResultCode::Ok;
      this->message = "";
      this->primaryKey = DataTypes::Indexing::Key();
    }

    ResultStatus(const ResultCode& code, const std::string&  message){
      this->code = ResultCode::Ok;
      this->message = message;
      this->primaryKey = DataTypes::Indexing::Key();
    }
  };
}

