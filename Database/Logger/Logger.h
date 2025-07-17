#pragma once
#include "../Constants.h"
#include "../Row/Row.h"
#include <cstdint>
#include <mutex>


namespace DatabaseEngine::Logging {

  enum OperationType : uint8_t{
    InvalidOperation = 0,
    Insert = 1,
    Update = 2,
    Delete = 3,
  };

  struct CheckPoint {
    Constants::transaction_id_t transactionId;
    Constants::log_sequence_number_t logSequenceNumber;
    off_t logFileOffset; // Offset in the log file where the checkpoint is written
    uint32_t checkSum;

    static constexpr uint32_t Size();
    [[nodiscard]] uint32_t static CalculateCheckSum(const CheckPoint& checkpoint);

    CheckPoint();
    CheckPoint(const Constants::transaction_id_t& transactionId,
               const Constants::log_sequence_number_t& logSequenceNumber,
               const off_t& logFileOffset);
  };

  struct Transaction {
    Constants::transaction_id_t transactionId = INVALID_TRANSACTION_ID;
    OperationType operation;
    Constants::table_id_t tableOrdinalPosition; //in master db
    Constants::page_id_t pageId;
    int rowIndex;

    bool hasOldRow; // Indicates if the old row exists
    StorageTypes::Row* oldRow;

    bool hasNewRow; // Indicates if the new row exists
    StorageTypes::Row* newRow;

    Constants::log_sequence_number_t logSequenceNumber; // Sequence number for the log entry

    Transaction();
    Transaction(const Constants::transaction_id_t& transactionId,
                const Constants::log_sequence_number_t& logSequenceNumber,
                const OperationType& operation,
                const Constants::table_id_t& tableOrdinalPosition,
                const Constants::page_id_t& pageId,
                const int& rowIndex,
                StorageTypes::Row* oldRow = nullptr,
                StorageTypes::Row* newRow = nullptr);

    [[nodiscard]] int GetSize()const;
    [[nodiscard]] int GetStaticDataSize()const;
  };

  class Logger final {
    int logFileDescriptor;
    int checkPointFileDescriptor;

    Constants::transaction_id_t currentTransactionId;
    Dictionary<Constants::transaction_id_t, Constants::log_sequence_number_t> transactionLogSequenceNumbers;
    std::mutex transactionLogMutex;

    void FlushLogDescriptor()const;

    void FlushCheckPointDescriptor()const;

    static void SerializeTransaction(std::vector<char>* buffer, const Transaction& transaction);

    static void DeserializeTransactionHeader(
      const std::vector<char>& buffer,
      Transaction& transaction,
      uint32_t& pos);

    static void DeserializeTransactionBody(
      const std::vector<char>& buffer,
      Transaction& transaction,
      const std::vector<StorageTypes::Table*> &tables,
      uint32_t& pos);

    static StorageTypes::Row* DeserializeRow(const std::vector<char>& buffer, uint32_t& pos, const StorageTypes::Table* table);

    static void SerializeRow(std::vector<char> *buffer, uint32_t& pos, StorageTypes::Row *row);

    [[nodiscard]] CheckPoint RecoverLastCheckPoint() const;

    void SetCurrentTransactionId(const Constants::transaction_id_t& transactionId);

  public:
      explicit Logger(const std::string& logFilePath);
      ~Logger();

      Logger(const Logger&) = delete;
      Logger(Logger&&) = delete;
      Logger& operator=(const Logger&) = delete;
      Logger& operator=(Logger&&) = delete;

      [[nodiscard]]CheckPoint Log(const Transaction& transaction)const;

      [[nodiscard]] Transaction CreateTransaction(
            const Constants::transaction_id_t& transactionId,
            const OperationType& operation,
            const Constants::table_id_t& tableOrdinalPosition,
            const Constants::page_id_t& pageId,
            const int& rowIndex,
            StorageTypes::Row* oldRow = nullptr,
            StorageTypes::Row* newRow = nullptr);

     [[nodiscard]] Constants::transaction_id_t StartTransaction();

    void LogCheckPoint(CheckPoint& checkPoint)const;

    void RecoverLogs(const std::vector<StorageTypes::Table*>& tables);
  };


}