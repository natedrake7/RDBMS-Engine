#include "../../include/Schedulers/GarbageCollector.h"
#include <bits/this_thread_sleep.h>

#include "../../include/Managers/TransactionManager.h"
#include "../../include/SystemDatabases/VersionDatabase.h"

namespace CoreEngine {
    using namespace std::chrono_literals;

    void GarbageCollector::Collect(const std::atomic<bool>& isServerRunning){
        auto& transactionManager = TransactionManager::Get();
        static const auto& versionDatabase = VersionDatabase::Get();

        extent_id_t lastScannedExtentId = 0;

        while (isServerRunning) {
            std::this_thread::sleep_for(20000ms);

            if (!versionDatabase.HasPendingVersions())
                continue;

            const auto oldestTransactionId = transactionManager.GetOldestActiveTransactionId();
            lastScannedExtentId = versionDatabase.CleanupVersionedData(oldestTransactionId, lastScannedExtentId);
        }
    }
}
