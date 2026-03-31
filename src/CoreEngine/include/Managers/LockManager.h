#pragma once
#include <shared_mutex>
#include <string>
#include <memory>

#include "DataStructures/Dictionary.h"
#include "DataTypes/DataTypes.h"

namespace Pages
{
    class PageView;
}

namespace CoreEngine::Lock {

  enum class LockType {
    Shared = 0,
    Exclusive = 1
  };

  enum class ResourceType : uint8_t {
    Row = 0,
    Page = 1,
    Table = 2,
    Database = 3
  };

  struct Resource{
    uint32_t resourceId;
    ResourceType type;
  };

  struct Lock{
    std::shared_mutex mutex;

    LockType type;
    Resource resource;
  };

  class LockManager {
    LockManager() = default;
    ~LockManager();

    Dictionary<std::string, Lock*> resources;
    std::shared_mutex locksMutex;

    static void LockResourceByType(Lock* lock);
    std::shared_ptr<Pages::PageView> GetPagePointer(const std::string& key, Pages::PageView *page);

  public:
    LockManager(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager& operator=(LockManager&&) = delete;
    
    static LockManager& Get() {
      static LockManager instance;

      return instance;
    }

    void Release(const std::string& key);

    std::shared_ptr<Pages::PageView> GetPage(const std::string& filename, const page_id_t & pageId, const ResourceType& resourceType, const LockType& lockType);
//    std::shared_ptr<Pages::Page> GetLargeObjectPage(const string& filename, const page_id_t & pageId);
//    std::shared_ptr<Pages::Page> GetIndexPage(const string& filename, const page_id_t & pageId);
//    std::shared_ptr<Pages::Page> GetHeaderPage(const string& filename, const page_id_t & pageId);
  };
}
