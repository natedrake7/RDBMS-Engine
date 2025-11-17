#pragma once
#include <mutex>
#include "../Constants.h"
#include "../../Systemic/DataStructures/SortedDictionary/SortedDictionary.h"
#include "../../Systemic/Network/Session.h"

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

  Constants::transaction_id_t BeginTransaction(const DataTypes::Guid& sessionId);
  void SetTransactionId(const Constants::transaction_id_t& transactionId);
  void CommitTransaction(const Constants::transaction_id_t& transactionId);
  void RollbackTransaction(const Constants::transaction_id_t& transactionId);
  Constants::transaction_id_t GetOldestActiveTransactionId();
};

} // DatabaseEngine
