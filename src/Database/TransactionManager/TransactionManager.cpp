#include "TransactionManager.h"

#include <ranges>

namespace DatabaseEngine {
   TransactionManager::TransactionManager(){
    this->currentTransactionId = 0;
  }

  TransactionManager::~TransactionManager() = default;

  TransactionManager& TransactionManager::Get(){
    static TransactionManager instance;

    return instance;
  }

  QueryPipeline::PhysicalPlan::Snapshot TransactionManager::BeginTransaction(const DataTypes::Guid& sessionId){
    QueryPipeline::PhysicalPlan::Snapshot snapshot;

    {
      std::unique_lock<std::mutex> lock(this->transactionMutex);

      snapshot.maximumTransactionId = this->currentTransactionId;
      snapshot.transactionId = this->currentTransactionId++;
    }

    {
      std::unique_lock<std::mutex> lock(this->dictionaryMutex);

      snapshot.minimumTransactionId = this->activeTransactions.FirstOrDefault().transactionId;

      for (const auto &[activeTransactionId, _] : this->activeTransactions | views::values)
        snapshot.activeTransactionIds.Add(activeTransactionId);

      this->activeTransactions.Add(snapshot.transactionId,
        TransactionInfo{
          snapshot.transactionId,
          sessionId
      });
    }

    return snapshot;
  }

  void TransactionManager::SetTransactionId(const Constants::transaction_id_t &transactionId) {
    std::unique_lock<std::mutex> lock(this->transactionMutex);

    this->currentTransactionId = transactionId;
  }

  void TransactionManager::CommitTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot) {
    std::unique_lock<std::mutex> lock(this->dictionaryMutex);

    this->activeTransactions.Remove(snapshot.transactionId);
  }

  void TransactionManager::RollbackTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot){
    {
      std::unique_lock<std::mutex> lock(this->dictionaryMutex);

      this->activeTransactions.Remove(snapshot.transactionId);
    }

    //apply rollback mechanism
  }

  Constants::transaction_id_t TransactionManager::GetOldestActiveTransactionId() {
    std::unique_lock<std::mutex> lock(this->dictionaryMutex);

    return this->activeTransactions.FirstOrDefault().transactionId;
  }
}