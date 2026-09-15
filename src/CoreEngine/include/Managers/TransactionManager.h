#pragma once
#include <mutex>
#include "../../../QueryPipeline/include/PhysicalPlan.h"
#include "../../../Systemic/include/DataStructures/SortedDictionary.h"

namespace CoreEngine {
struct ModificationInfo {
    Int databaseId;
    Int tableId;
    std::vector<DataTypes::RowIdentifier> rows;
};

struct TransactionInfo {
    transaction_id_t transactionId;
    session_id_t sessionId;
    ModificationInfo modificationInfo;
};


class TransactionManager {
    std::mutex transactionMutex;
    transaction_id_t currentTransactionId;

    std::mutex dictionaryMutex;
    SortedDictionary<transaction_id_t, TransactionInfo> activeTransactions;

    TransactionManager();

public:
    static TransactionManager& Get();

    Snapshot BeginTransaction(session_id_t sessionId);
    void SetTransactionId(transaction_id_t transactionId);
    void CommitTransaction(const Snapshot& snapshot);
    void RollbackTransaction(const ExecutionContext& context);
    transaction_id_t GetOldestActiveTransactionId();
};

} // DatabaseEngine
