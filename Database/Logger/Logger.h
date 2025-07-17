#pragma once
#include "../Constants.h"
#include "../Row/Row.h"
#include "./Logger.Structures.h"
#include <cstdint>
#include <mutex>


namespace DatabaseEngine::Logging {

  enum OperationType : uint8_t{
    InvalidOperation = 0,
    InsertRow = 1,
    UpdateRow = 2,
    DeleteRow = 3,
    CreateTable = 4,
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


  struct LogEntry  {
    Constants::transaction_id_t transactionId;
    OperationType operation;
    Constants::table_id_t tableOrdinalPosition; //in master db
    Constants::page_id_t pageId;
    int rowIndex;

    LoggingStructures::LogEntryBody* body;

    Constants::log_sequence_number_t logSequenceNumber; // Sequence number for the log entry

    LogEntry();
    LogEntry(const Constants::transaction_id_t& transactionId,
                const Constants::log_sequence_number_t& logSequenceNumber,
                const OperationType& operation,
                const Constants::table_id_t& tableOrdinalPosition,
                const Constants::page_id_t& pageId,
                const int& rowIndex,
                LoggingStructures::LogEntryBody* body);

    ~LogEntry();

    [[nodiscard]] int GetSize()const;
    [[nodiscard]] int GetStaticDataSize()const;
    void AllocateBody();
  };


  class Logger final {
    int logFileDescriptor;
    int checkPointFileDescriptor;

    Constants::transaction_id_t currentTransactionId;
    Dictionary<Constants::transaction_id_t, Constants::log_sequence_number_t> transactionLogSequenceNumbers;
    std::mutex transactionLogMutex;

    void FlushLogDescriptor()const;

    void FlushCheckPointDescriptor()const;

    static void SerializeTransaction(std::vector<char>* buffer, const LogEntry& transaction);

    static void DeserializeLogEntryHeader(
      const std::vector<char>& buffer,
      LogEntry& transaction,
      uint32_t& pos);

    static void DeserializeLogEntryBody(
      const std::vector<char>& buffer,
      const LogEntry& transaction,
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

      [[nodiscard]]CheckPoint Log(const LogEntry& transaction)const;

      [[nodiscard]] LogEntry CreateLogEntry(
            const Constants::transaction_id_t& transactionId,
            const OperationType& operation,
            const Constants::table_id_t& tableOrdinalPosition,
            const Constants::page_id_t& pageId,
            const int& rowIndex,
            LoggingStructures::LogEntryBody* body);

     [[nodiscard]] Constants::transaction_id_t StartTransaction();

    void LogCheckPoint(CheckPoint& checkPoint)const;

    void RecoverLogs(const std::vector<StorageTypes::Table*>& tables);
  };


}