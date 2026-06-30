#include "../../include/Managers/TransactionManager.h"

#include <ranges>

#include "../../../Server/include/Server.h"
#include "Contexts/ExecutionContext.h"

namespace CoreEngine {
  TransactionManager::TransactionManager(){
    this->currentTransactionId = 0;
  }

  TransactionManager::~TransactionManager() = default;

  TransactionManager& TransactionManager::Get(){
    static TransactionManager instance;

    return instance;
  }

  Snapshot TransactionManager::BeginTransaction(const DataTypes::Guid& sessionId){
    Snapshot snapshot;

    {
      std::unique_lock lock(this->transactionMutex);

      snapshot.maximumTransactionId = this->currentTransactionId;
      snapshot.transactionId = this->currentTransactionId++;
    }

    {
      std::unique_lock lock(this->dictionaryMutex);

      snapshot.minimumTransactionId = this->activeTransactions.FirstOrDefault().transactionId;

      for (const auto &[activeTransactionId, _, modifications] : this->activeTransactions | std::views::values)
        snapshot.activeTransactionIds.Add(activeTransactionId);

      this->activeTransactions.Add(snapshot.transactionId, TransactionInfo(
            snapshot.transactionId,
            sessionId
        ));
    }

    return snapshot;
  }

  void TransactionManager::SetTransactionId(const transaction_id_t transactionId) {
    std::unique_lock lock(this->transactionMutex);

    this->currentTransactionId = transactionId;
  }

  void TransactionManager::CommitTransaction(const Snapshot& snapshot) {
    std::unique_lock lock(this->dictionaryMutex);

    this->activeTransactions.Remove(snapshot.transactionId);
  }

  void TransactionManager::RollbackTransaction(const ExecutionContext& context){
    TransactionInfo transactionInfo;

    {
      std::unique_lock lock(this->dictionaryMutex);

      transactionInfo = std::move(this->activeTransactions.Get(context.GetCurrentTransactionId()));

      this->activeTransactions.Remove(context.GetCurrentTransactionId());
    }

    static auto& server = Network::Server::Get();
    // const auto* db = server.UseDatabase(context, transactionInfo.modificationInfo.databaseId);
    // const auto* table = db->OpenTableById(transactionInfo.modificationInfo.tableId);

    //TODO track index keys along with rids to rollback index entries as well
    // for (const auto& rowId : transactionInfo.modificationInfo.rows)
    //   table->Rollback(snapshot, rowId);

    //apply rollback mechanism
  }

  transaction_id_t TransactionManager::GetOldestActiveTransactionId() {
    std::unique_lock lock(this->dictionaryMutex);

    return this->activeTransactions.FirstOrDefault().transactionId;
  }
}
