#include "../../include/Managers/IdentityManager.h"

#include "../../include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"


namespace DatabaseEngine::StorageTypes {
  BigInt IdentityManager::Generate(){
    bool updateMasterDb = false;

    MultiThreading::WriterGuard guard(&this->mutex);

    const auto value = this->header.lastValue;

    this->header.lastValue += this->header.increment;

    updateMasterDb = value >= (this->startingValue + this->header.cacheBlock);

    if (updateMasterDb) {
      this->startingValue = value;
      this->UpdateMasterDb(value);
    }

    return value;
  }

   void IdentityManager::UpdateMasterDb(const BigInt value) const{
     SystemCatalog::Get().UpdateIdentityByColumnId(this->header.tableId, this->header.columnId, value + this->header.increment);
   }

  IdentityManager::IdentityManager() {
    this->startingValue = 0;
  }

  IdentityManager::~IdentityManager() = default;

  void IdentityManager::SetHeaderIds(const Int tableId, const Int columnId){
    this->header.tableId = tableId;
    this->header.columnId = columnId;
  }

  void IdentityManager::SetHeader(const Headers::IdentityColumnsHeader &newHeader){
    this->header = newHeader;
    this->startingValue = this->header.lastValue;
  }

  const Headers::IdentityColumnsHeader & IdentityManager::GetHeader() const{
    return this->header;
  }

  bool IdentityManager::TryGenerate(BigInt& value){
    if (this->header.columnId == INVALID_COLUMN_ID)
      return false;

    value = this->Generate();

    return true;
  }

  void IdentityManager::UpdateMasterDb()const{
    if (this->header.columnId == INVALID_COLUMN_ID)
      return;

    SystemCatalog::Get().UpdateIdentityByColumnId(this->header.tableId, this->header.columnId, this->header.lastValue);
  }

  bool IdentityManager::IsValid() const{ return this->header.columnId != INVALID_COLUMN_ID; }
}
