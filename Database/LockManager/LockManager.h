#pragma once
#include "../Constants.h"
#include <shared_mutex>
#include <string>


namespace DatabaseEngine::Lock {

enum class LockType {
  Shared = 0,
  Exclusive = 1
};

struct TableLockKey {
    string databaseName;
    string tableName;
    LockType type;

  bool operator==(const TableLockKey &other) const;
    virtual ~TableLockKey() = default;
  };

  struct PageLockKey : TableLockKey{
    Constants::page_id_t pageId;

    bool operator==(const PageLockKey& other) const;
  };

  struct RowLockKey final : PageLockKey{
    uint16_t rowId;

    bool operator==(const RowLockKey& other) const;
  };

  class LockManager {
    LockManager() = default;
    ~LockManager() = default;

    // Dictionary<TableLockKey, shared_mutex> locks;
    std::shared_mutex locksMutex;

    void AddKeyToDictionary(const TableLockKey& lockKey);

  public:
    LockManager(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager& operator=(LockManager&&) = delete;
    
    static LockManager& Get() {
      static LockManager instance;

      return instance;
    }

    void Lock(const TableLockKey& lockKey);
    void Unlock(const TableLockKey& lockKey);
  };
}


template<>
  struct std::hash<DatabaseEngine::Lock::TableLockKey> {
    std::size_t operator()(const DatabaseEngine::Lock::TableLockKey& k) const noexcept {
      return hash<std::string>()(k.databaseName) ^ (hash<std::string>()(k.tableName) << 1);
    }
  };