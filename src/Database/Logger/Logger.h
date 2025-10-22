#pragma once
#include "../Constants.h"
#include "../Row/Row.h"
#include "./Logger.Structures.h"
#include "../../Systemic/DataStructures/HashSet/HashSet.h"
#include <cstdint>
#include <mutex>

namespace DatabaseEngine::Logging {

  enum OperationType : uint8_t{
    InvalidOperation = 0,
    InsertRow = 1,
    UpdateRow = 2,
    DeleteRow = 3,
    CreateTable = 4,
    AlterTable = 5,
    DropTable = 6,
    //etc...
  };

  static const Dictionary<OperationType, std::string> OperationTypeToString = {
    { OperationType::InvalidOperation, "Invalid Operation"},
    { OperationType::InsertRow, "Insert Row"},
    { OperationType::UpdateRow, "Update Row"},
    { OperationType::DeleteRow, "Delete Row"},
    { OperationType::CreateTable, "Create Table"}
  };

  static const HashSet<OperationType> RowAffectedOperationTypes = {
    OperationType::InsertRow,
    OperationType::UpdateRow,
    OperationType::DeleteRow
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

    LoggingStructures::LogEntryBody* body;

    Constants::log_sequence_number_t logSequenceNumber; // Sequence number for the log entry

    LogEntry();
    LogEntry(const Constants::transaction_id_t& transactionId,
                const Constants::log_sequence_number_t& logSequenceNumber,
                const OperationType& operation,
                const Constants::table_id_t& tableOrdinalPosition,
                LoggingStructures::LogEntryBody* body);

    ~LogEntry();

    [[nodiscard]] int GetSize()const;
    [[nodiscard]] constexpr int GetStaticDataSize()const;
    void DeserializeHeader(const std::vector<char>& buffer, uint32_t& pos);
    void Serialize(std::vector<char>* buffer)const;
    void AllocateBody();

    [[nodiscard]] bool ValidateIntegrity()const;

    [[nodiscard]] StorageTypes::Row* GetRow()const;

    friend ostream& operator<<(ostream& stream, const LogEntry& logEntry);
  };


  class Logger {
    protected:
      int logFileDescriptor;

      Constants::transaction_id_t currentTransactionId;
      Dictionary<Constants::transaction_id_t, Constants::log_sequence_number_t> transactionLogSequenceNumbers;
      std::mutex transactionLogMutex;

      void FlushLogDescriptor()const;

      void SetCurrentTransactionId(const Constants::transaction_id_t& transactionId);

  public:
      explicit Logger(const std::string& logFilePath);
      virtual ~Logger();

      Logger(const Logger&) = delete;
      Logger(Logger&&) = delete;
      Logger& operator=(const Logger&) = delete;
      Logger& operator=(Logger&&) = delete;

      [[nodiscard]]CheckPoint Log(const LogEntry& logEntry)const;

      [[nodiscard]] LogEntry CreateLogEntry(
            const Constants::transaction_id_t& transactionId,
            const OperationType& operation,
            const Constants::table_id_t& tableOrdinalPosition,
            LoggingStructures::LogEntryBody* body);

     [[nodiscard]] Constants::transaction_id_t StartTransaction();

    virtual std::vector<LogEntry>  RecoverLogs(const std::vector<StorageTypes::Table*>& tables);
  };
}