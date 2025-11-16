#include "TransactionManager.h"

namespace DatabaseEngine {
   TransactionManager::TransactionManager(){
    this->currentTransactionId = 0;
  }

  TransactionManager::~TransactionManager() = default;

  TransactionManager& TransactionManager::Get(){
    static TransactionManager instance;

    return instance;
  }

  Constants::transaction_id_t TransactionManager::BeginTransaction(const DataTypes::Guid& sessionId){
    Constants::transaction_id_t transactionId = 0;
    {
      std::unique_lock<std::mutex> lock(this->transactionMutex);

      transactionId = this->currentTransactionId++;
    }

    {
      std::unique_lock<std::mutex> lock(this->dictionaryMutex);

      this->activeTransactions.Add(transactionId,
        TransactionInfo{
          transactionId,
          sessionId
      });
    }

    return transactionId;
  }

  void TransactionManager::SetTransactionId(const Constants::transaction_id_t &transactionId) {
    std::unique_lock<std::mutex> lock(this->transactionMutex);

    this->currentTransactionId = transactionId;
  }

} // DatabaseEngine