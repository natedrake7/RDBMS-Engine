#pragma once
#include <mutex>
#include "../Constants.h"
#include "../../../QueryPipeline/include/PhysicalPlan.h"
#include "../../../Systemic/include/DataStructures/SortedDictionary.h"
#include "../../../Systemic/include/Security/Session.h"

namespace DatabaseEngine {

struct TransactionInfo {
  Constants::transaction_id_t transactionId;

  const DataTypes::Guid sessionId;
};

class TransactionManager {
  std::mutex transactionMutex;
  Constants::transaction_id_t currentTransactionId;

  std::mutex dictionaryMutex;
  SortedDictionary<Constants::transaction_id_t, TransactionInfo> activeTransactions;

  TransactionManager();
  ~TransactionManager();

public:
  static TransactionManager& Get();

  QueryPipeline::PhysicalPlan::Snapshot BeginTransaction(const DataTypes::Guid& sessionId);
  void SetTransactionId(const Constants::transaction_id_t& transactionId);
  void CommitTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot);
  void RollbackTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot);
  Constants::transaction_id_t GetOldestActiveTransactionId();
};

} // DatabaseEngine
