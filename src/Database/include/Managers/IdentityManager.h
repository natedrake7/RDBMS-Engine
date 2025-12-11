#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"

#include <mutex>

namespace DatabaseEngine::StorageTypes
{
  class IdentityManager {
    Headers::IdentityColumnsHeader header;
    int64_t startingValue;

    mutable MultiThreading::ReadWriteMutex mutex;

    [[nodiscard]] int64_t Generate();
    void UpdateMasterDb(const int64_t& value)const;

    public:
      IdentityManager();
      ~IdentityManager();

      void SetHeaderIds(const int32_t& tableId, const int32_t& columnId);
      void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
      [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

      [[nodiscard]] bool TryGenerate(int64_t& value);
      void UpdateMasterDb()const;

      [[nodiscard]] bool IsValid()const;
  };
}