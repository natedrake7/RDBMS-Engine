#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"

#include <mutex>

namespace DatabaseEngine::StorageTypes
{
  class IdentityManager {
    Headers::IdentityColumnsHeader header;
    BigInt startingValue;

    mutable MultiThreading::ReadWriteMutex mutex;

    [[nodiscard]] BigInt Generate();
    void UpdateMasterDb(BigInt value)const;

    public:
      IdentityManager();
      ~IdentityManager();

      void SetHeaderIds(Int tableId, Int columnId);
      void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
      [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

      [[nodiscard]] bool TryGenerate(BigInt& value);
      void UpdateMasterDb()const;

      [[nodiscard]] bool IsValid()const;
  };
}