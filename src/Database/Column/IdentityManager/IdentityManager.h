#pragma once
#include "../../../AdditionalLibraries/DataTypes/Headers/Headers.h"
#include <mutex>

namespace DatabaseEngine::StorageTypes
{
  class IdentityManager {
    Headers::IdentityColumnsHeader header;
    int64_t startingValue;

    bool valueChanged;

    std::mutex mutex;

    [[nodiscard]] int64_t Generate();
    void UpdateMasterDb(const int64_t& value)const;

    public:
      IdentityManager();
      ~IdentityManager();

      void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
      [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

      [[nodiscard]] bool TryGenerate(int64_t& value);
      void UpdateMasterDb()const;

      [[nodiscard]] bool IsValid()const;
  };
}