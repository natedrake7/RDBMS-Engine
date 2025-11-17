#include "GarbageCollector.h"
#include <bits/this_thread_sleep.h>

#include "../Storage/StorageManager/StorageManager.h"
#include "../TransactionManager/TransactionManager.h"

namespace DatabaseEngine {
  void GarbageCollector::Collect(const std::atomic<bool>& isServerRunning){
    auto& transactionManager = TransactionManager::Get();
    auto& storageManager = Storage::StorageManager::Get();

    Constants::page_id_t lastScannedPageId = 0;

    while (isServerRunning) {
      const auto oldestTransactionId = transactionManager.GetOldestActiveTransactionId();

      std::this_thread::sleep_for(1000ms);
    }
  }

}