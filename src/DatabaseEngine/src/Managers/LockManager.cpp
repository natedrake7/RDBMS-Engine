#include "../../include/Managers/LockManager.h"
#include "../../include/BufferPool/StorageManager.h"

namespace DatabaseEngine::Lock {

  LockManager::~LockManager(){
    for(auto& [key, lock] : this->resources){
      delete lock;
    }
  }

  std::shared_ptr<Pages::PageView> LockManager::GetPage(
      const std::string& filename,
      const page_id_t & pageId,
      const ResourceType& resourceType,
      const LockType& lockType){
    const auto key = filename + std::to_string(pageId);

    this->locksMutex.lock_shared();

    if(this->resources.Contains(key)){
      Lock* lock = this->resources.Get(key);

      if(lock->type == LockType::Exclusive){
        this->locksMutex.unlock_shared();

        //wait for lock to be released

        return this->GetPage(filename, pageId, resourceType, lockType);
      }

      this->locksMutex.unlock_shared();

      if(lockType == LockType::Exclusive){

        lock->mutex.lock();

        Pages::PageView* page = nullptr;

        return std::shared_ptr<Pages::PageView>(page);
      }

      if(lockType == LockType::Shared){
        lock->mutex.lock_shared();

        Pages::PageView* page = nullptr;

        return std::shared_ptr<Pages::PageView>(page);
      }
    }

    this->locksMutex.lock();

    if(this->resources.Contains(key)){
      this->locksMutex.unlock();

      return this->GetPage(filename, pageId, resourceType, lockType);
    }

    Lock* lock = new Lock();
    lock->type = lockType;
    lock->resource.resourceId = pageId;
    lock->resource.type = resourceType;

    this->resources.Add(key, lock);

    LockManager::LockResourceByType(lock);

    Pages::PageView* page = nullptr;//Storage::StorageManager::Get().GetPage(filename, pageId, );

    return std::shared_ptr<Pages::PageView>(page);
  }

  void LockManager::LockResourceByType(Lock *lock){
    switch (lock->type) {
      case LockType::Shared:
        lock->mutex.lock_shared();
        return;
      case LockType::Exclusive:
        lock->mutex.lock();
        return;
      default:
        throw std::runtime_error("Invalid Lock type");
    }
  }

  void LockManager::Release(const std::string & key){

  }

  std::shared_ptr<Pages::PageView> LockManager::GetPagePointer(const std::string& key, Pages::PageView *page) {
      auto deleter = [this, key](Pages::PageView* p) {
          this->Release(key);
      };

      return {page, deleter};
  }

}
