#include "LockManager.h"

namespace DatabaseEngine::Lock {
  bool TableLockKey::operator==(const TableLockKey &other) const{
    return this->databaseName == other.databaseName
          && this->tableName == other.tableName;
  }

  bool PageLockKey::operator==(const PageLockKey &other)const{
      return TableLockKey::operator==(other) && pageId == other.pageId;
  }

  bool RowLockKey::operator==(const RowLockKey &other) const{
    return PageLockKey::operator==(other) && rowId == other.rowId;
  }

  void LockManager::AddKeyToDictionary(const TableLockKey &lockKey){

    // this->locksMutex.lock();
    //
    //   if (!this->locks.Contains(lockKey))
    //     this->locks.Add(lockKey, shared_mutex());
    //
    //   this->locks.Add(lockKey, shared_mutex());
    //
    // this->locksMutex.unlock();
  }

void LockManager::Lock(const TableLockKey &lockKey){

    //check by type (table lock key has higher precedence than other locks)
    // this->locksMutex.lock_shared();
    // if (!this->locks.Contains(lockKey)) {
    //   this->locksMutex.unlock_shared();
    //   this->AddKeyToDictionary(lockKey);
    // }
    //
    // this->locksMutex.lock_shared();
    //
    // shared_mutex * mutexPtr = &this->locks.Get(lockKey);
    //
    // switch (lockKey.type){
    //   case LockType::Shared:
    //       mutexPtr->lock_shared();
    //     break;
    //   case LockType::Exclusive:
    //       mutexPtr->lock();
    //     break;
    //   default: {
    //     this->locksMutex.unlock_shared();
    //     throw runtime_error("Lock type not supported");
    //   }
    // }
    //
    // this->locksMutex.unlock_shared();
  }

  void LockManager::Unlock(const TableLockKey &lockKey){
    // this->locksMutex.lock_shared();
    // if (!this->locks.Contains(lockKey)) {
    //   this->locksMutex.unlock_shared();
    //   return;
    // }
    //
    // shared_mutex * mutexPtr = &this->locks.Get(lockKey);
    // switch (lockKey.type){
    //   case LockType::Shared:
    //     mutexPtr->unlock_shared();
    //     break;
    //   case LockType::Exclusive:
    //     mutexPtr->unlock();
    //     break;
    //   default: {
    //     this->locksMutex.unlock_shared();
    //     throw runtime_error("Lock type not supported");
    //   }
    // }
    //
    // this->locksMutex.unlock_shared();
  }
}
