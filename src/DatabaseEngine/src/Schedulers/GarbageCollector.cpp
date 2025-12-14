#include "../../include/Schedulers/GarbageCollector.h"
#include <bits/this_thread_sleep.h>

#include "../../../Server/include/Server.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Managers/TransactionManager.h"

namespace DatabaseEngine {
  void GarbageCollector::Collect(const std::atomic<bool>& isServerRunning){
    auto& transactionManager = TransactionManager::Get();
    const auto* versionDatabase = Network::Server::Get().GetVersionDatabase();

    extent_id_t lastScannedExtentId = 0;

    while (isServerRunning) {
      std::this_thread::sleep_for(20000ms);
      const auto oldestTransactionId = transactionManager.GetOldestActiveTransactionId();
      lastScannedExtentId = versionDatabase->CleanupVersionedData(oldestTransactionId, lastScannedExtentId);
    }
  }
//CREATE TABLE dbo.Actors(ID INT PRIMARY KEY IDENTITY(1,1), Name STRING(200), Age INT NOT NULL)
//insert into dbo.aCTORS(Name, Age) VALUES('Kostas', 10)
}