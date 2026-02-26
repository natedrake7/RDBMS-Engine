#include "../../include/Schedulers/GarbageCollector.h"
#include <bits/this_thread_sleep.h>

#include "../../../Server/include/Server.h"
#include "../../include/Managers/TransactionManager.h"
#include "Memory/Allocator.h"

namespace DatabaseEngine {
    using namespace std::chrono_literals;

    void GarbageCollector::Collect(const std::atomic<bool>& isServerRunning){
        auto& transactionManager = TransactionManager::Get();
        static const auto& versionDatabase = VersionDatabase::Get();

        extent_id_t lastScannedExtentId = 0;

        while (isServerRunning) {
            std::this_thread::sleep_for(20000ms);

            const Memory::Allocator allocator;
            const auto oldestTransactionId = transactionManager.GetOldestActiveTransactionId();
            lastScannedExtentId = versionDatabase.CleanupVersionedData(&allocator, oldestTransactionId, lastScannedExtentId);
        }
    }
}
