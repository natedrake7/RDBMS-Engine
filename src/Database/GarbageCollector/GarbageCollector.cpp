#include "GarbageCollector.h"
#include <bits/this_thread_sleep.h>

#include "../../Server/Server.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../TransactionManager/TransactionManager.h"

namespace DatabaseEngine {
  void GarbageCollector::Collect(const std::atomic<bool>& isServerRunning){
    auto& transactionManager = TransactionManager::Get();
    auto* versionDatabase = Server::ServerInstance::Get().GetVersionDatabase();

    Constants::page_id_t lastScannedPageId = 0;
    Constants::extent_id_t lastScannedExtentId = 0;

    std::this_thread::sleep_for(10000ms);
    while (isServerRunning) {
      const auto oldestTransactionId = transactionManager.GetOldestActiveTransactionId();

      lastScannedExtentId = versionDatabase->CleanupVersionedData(oldestTransactionId, lastScannedExtentId);

      std::this_thread::sleep_for(1000ms);
    }
  }

}