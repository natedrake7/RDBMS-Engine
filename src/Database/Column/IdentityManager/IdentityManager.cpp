#include "IdentityManager.h"
#include "../../../Server/Server.h"


namespace DatabaseEngine::StorageTypes {
  IdentityManager::IdentityManager() {
    this->startingValue = 0;
    this->valueChanged = false;
  }

   IdentityManager::~IdentityManager() = default;

  int64_t IdentityManager::Generate(){
    bool updateMasterDb = false;

    this->mutex.lock();
      const auto value = this->header.lastValue;

      this->header.lastValue += this->header.increment;

      this->valueChanged = true;

      updateMasterDb = value >= this->startingValue + this->header.cacheBlock;

      if (updateMasterDb)
        this->startingValue = value;

    this->mutex.unlock();

    if (updateMasterDb)
      this->UpdateMasterDb();

    return value;
  }

  bool IdentityManager::TryGenerate(int64_t &value){
    if (this->header.columnId == Constants::INVALID_COLUMN_ID)
      return false;

    value = this->Generate();

    return true;
  }

  void IdentityManager::UpdateMasterDb(const int64_t &value) const{
    if (this->header.columnId == Constants::INVALID_COLUMN_ID)
      return;

    Server::ServerInstance::Get().UpdateIdentityByColumnId(this->header.tableId, this->header.columnId, value + 1);
  }

  void IdentityManager::UpdateMasterDb()const{
    if (this->header.columnId == Constants::INVALID_COLUMN_ID)
      return;

    const auto value = this->valueChanged
        ? this->header.lastValue + 1
        : this->header.lastValue;

    Server::ServerInstance::Get().UpdateIdentityByColumnId(this->header.tableId, this->header.columnId, value);
  }

  void IdentityManager::SetHeader(const Headers::IdentityColumnsHeader &newHeader){
    this->header = newHeader;
    this->startingValue = this->header.lastValue;
  }

  const Headers::IdentityColumnsHeader & IdentityManager::GetHeader() const{
    return this->header;
  }
}
