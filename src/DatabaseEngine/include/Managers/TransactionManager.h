#pragma once
#include <mutex>
#include "../Constants.h"
#include "../../../QueryPipeline/include/PhysicalPlan.h"
#include "../../../Systemic/include/DataStructures/SortedDictionary.h"

namespace DatabaseEngine {

struct TransactionInfo {
  transaction_id_t transactionId;

  const DataTypes::Guid sessionId;
};

class TransactionManager {
  std::mutex transactionMutex;
  transaction_id_t currentTransactionId;

  std::mutex dictionaryMutex;
  SortedDictionary<transaction_id_t, TransactionInfo> activeTransactions;

  TransactionManager();
  ~TransactionManager();

public:
  static TransactionManager& Get();

  Snapshot BeginTransaction(const DataTypes::Guid& sessionId);
  void SetTransactionId(const transaction_id_t& transactionId);
  void CommitTransaction(const Snapshot& snapshot);
  void RollbackTransaction(const Snapshot& snapshot);
  transaction_id_t GetOldestActiveTransactionId();
};

} // DatabaseEngine
