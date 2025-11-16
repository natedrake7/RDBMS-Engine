#include "Logger.h"
#include "../../Systemic/DataStructures/BitMap/BitMap.h"
#include "../Table/Table.h"
#include <cstring>
#include <fcntl.h>
#include <iostream>
#ifdef _WIN32
  #include <io.h>
#else
  #include <unistd.h>
#endif

namespace DatabaseEngine::Logging {
  Logger::Logger(const std::string& logFilePath){
    const std::string logFileName = logFilePath + ".wal";

    this->logFileDescriptor = ::open(logFileName.c_str(), O_RDWR | O_CREAT, 0644);

    if (this->logFileDescriptor < 0)
      throw std::runtime_error("Failed to open or create log file: " + logFilePath);

    this->currentTransactionId = 0;
  }

  Logger::~Logger(){
    if (this->logFileDescriptor >= 0)
      ::close(this->logFileDescriptor);
  }

uint32_t CheckPoint::CalculateCheckSum(const CheckPoint& checkpoint){
    const auto* data = reinterpret_cast<const uint8_t*>(&checkpoint);

    const size_t size = CheckPoint::Size() - sizeof(checkpoint.checkSum);
    uint32_t sum = 0;

    for (size_t i = 0; i < size; ++i)
        sum += data[i];

    return sum;
  }

  CheckPoint::CheckPoint(){
    this->transactionId = INVALID_TRANSACTION_ID;
    this->logSequenceNumber = 0;
    this->logFileOffset = 0;
    this->checkSum = 0;
  }

  CheckPoint::CheckPoint(
    const Constants::transaction_id_t &transactionId,
    const Constants::log_sequence_number_t &logSequenceNumber,
    const off_t &logFileOffset){
    this->transactionId = transactionId;
    this->logSequenceNumber = logSequenceNumber;
    this->logFileOffset = logFileOffset;
    this->checkSum = 0;
  }

  LogEntry::LogEntry() {
    this->transactionId = Constants::INVALID_TRANSACTION_ID;
    this->logSequenceNumber = Constants::INVALID_LOG_SEQUENCE_NUMBER;
    this->operation = OperationType::InvalidOperation;
    this->tableOrdinalPosition = Constants::INVALID_TABLE_ORDINAL_POS;

    this->body = nullptr;

  }

  LogEntry::LogEntry(
    const Constants::transaction_id_t &transactionId,
    const Constants::log_sequence_number_t& logSequenceNumber,
    const OperationType &operation,
    const Constants::table_id_t &tableOrdinalPosition,
    LoggingStructures::LogEntryBody* body){

    this->transactionId = transactionId;
    this->logSequenceNumber = logSequenceNumber;
    this->operation = operation;
    this->tableOrdinalPosition = tableOrdinalPosition;

    this->body = body;
  }

  LogEntry::~LogEntry(){
    delete this->body;
  }

  int LogEntry::GetSize()const{
    return this->GetStaticDataSize() + ((this->body != nullptr) ? this->body->GetSize() : 0);
  }

  constexpr int LogEntry::GetStaticDataSize()const {
    return  sizeof(this->transactionId) +
            sizeof(this->logSequenceNumber) +
            sizeof(this->operation) +
            sizeof(this->tableOrdinalPosition);
  }

  void LogEntry::DeserializeHeader(const std::vector<char> &buffer, uint32_t &pos){
      memcpy(&this->transactionId, buffer.data() + pos, sizeof(this->transactionId));
      pos += sizeof(this->transactionId);

      memcpy(&this->logSequenceNumber, buffer.data() + pos, sizeof(this->logSequenceNumber));
      pos += sizeof(this->logSequenceNumber);

      memcpy(&this->operation, buffer.data() + pos, sizeof(this->operation));
      pos += sizeof(this->operation);

      memcpy(&this->tableOrdinalPosition, buffer.data() + pos, sizeof(this->tableOrdinalPosition));
      pos += sizeof(this->tableOrdinalPosition);
  }

  void LogEntry::Serialize(std::vector<char> *buffer) const{
    uint32_t pos = 0;

    const auto transactionSize = this->GetSize();

    buffer->resize(transactionSize + sizeof(transactionSize));

    memcpy(buffer->data() + pos, &transactionSize, sizeof(transactionSize));
    pos += sizeof(transactionSize);
    memcpy(buffer->data() + pos, &this->transactionId, sizeof(this->transactionId));
    pos += sizeof(this->transactionId);
    memcpy(buffer->data() + pos, &this->logSequenceNumber, sizeof(this->logSequenceNumber));
    pos += sizeof(this->logSequenceNumber);
    memcpy(buffer->data() + pos, &this->operation, sizeof(this->operation));
    pos += sizeof(this->operation);
    memcpy(buffer->data() + pos, &this->tableOrdinalPosition, sizeof(this->tableOrdinalPosition));
    pos += sizeof(this->tableOrdinalPosition);

    this->body->Serialize(buffer, pos);
  }

  void LogEntry::AllocateBody(){
    switch (this->operation) {
      case InsertRow:
        this->body = new LoggingStructures::RowInsertBody();
        break;
      case UpdateRow:
        this->body = new LoggingStructures::RowUpdateBody();
        break;
      case DeleteRow:
        this->body = new LoggingStructures::RowDeleteBody();
        break;
      case CreateTable:
        this->body = new LoggingStructures::TableCreateBody();
        break;
      case InvalidOperation:
      default:
        this->body = nullptr;
        std::cerr << "Unknown operation type: " << OperationTypeToString.Get(this->operation)
                  << " . Log recovery cannot proceed." << std::endl;
        break;
    }
  }

  bool LogEntry::ValidateIntegrity() const{
    if (this->transactionId == INVALID_TRANSACTION_ID) {
      std::cerr << "Invalid Transaction ID on recovery log."
                  << " The current log will be skipped"<< std::endl;
      return false;
    }

    if (this->logSequenceNumber == INVALID_LOG_SEQUENCE_NUMBER) {
      std::cerr << "Invalid Log Sequence Number on recovery log with Transaction ID: "
                  << this->transactionId << std::endl;

      return false;
    }

    if (this->tableOrdinalPosition == Constants::INVALID_TABLE_ORDINAL_POS) {
      std::cerr << "Invalid table ordinal position on recovery log with Transaction ID: "
                  << this->transactionId << " and Log Sequence Number: "
                  << this->logSequenceNumber << std::endl;

      return false;
    }

    if (this->operation == Logging::OperationType::InvalidOperation) {
      std::cerr << "Invalid operation on recovery log with Transaction ID: "
                  << this->transactionId << " and Log Sequence Number: "
                  << this->logSequenceNumber << std::endl;

      return false;
    }

    return true;
  }

  StorageTypes::Row * LogEntry::GetRow() const{ return this->body->GetLastRowStatus(); }

  void Logger::FlushLogDescriptor()const{
    #ifdef _WIN32
        _commit(this->logFileDescriptor);
    #else
        ::fsync(this->logFileDescriptor);
    #endif
  }

  CheckPoint Logger::Log(const LogEntry &logEntry)const{
    std::vector<char> buffer;
    logEntry.Serialize(&buffer);

    const auto fileOffset = lseek(this->logFileDescriptor, 0, SEEK_END);

    const auto result = ::write(this->logFileDescriptor, buffer.data(), buffer.size());

    this->FlushLogDescriptor();

    if (result < 0 || result != buffer.size()) {
      std::cerr << "Failed to write transaction to log file" << std::endl;
    }

    return { logEntry.transactionId, logEntry.logSequenceNumber, fileOffset };
  }

  LogEntry Logger::CreateLogEntry(
      const Constants::transaction_id_t& transactionId,
      const OperationType &operation,
      const Constants::table_id_t& tableOrdinalPosition,
      LoggingStructures::LogEntryBody* body) {

    log_sequence_number_t logSequenceNumber = 0;

    if (this->transactionLogSequenceNumbers.TryGetValue(transactionId, logSequenceNumber))
      this->transactionLogSequenceNumbers.Update(transactionId, logSequenceNumber + 1);
    else
      this->transactionLogSequenceNumbers.Add(transactionId, 0);

    return {
      transactionId,
      logSequenceNumber,
      operation,
      tableOrdinalPosition,
      body
    };
  }

  Constants::transaction_id_t Logger::StartTransaction(){
    std::unique_lock<std::mutex> lock(this->transactionLogMutex);

    this->transactionLogSequenceNumbers.Add(this->currentTransactionId, 0);

    return this->currentTransactionId++;
  }

  std::vector<LogEntry>  Logger::RecoverLogs(const std::vector<StorageTypes::Table*>& tables){
    return {};
  }

  void Logger::SetCurrentTransactionId(const Constants::transaction_id_t &transactionId){
    std::unique_lock<std::mutex> lock(this->transactionLogMutex);

    this->currentTransactionId = transactionId;
  }

 constexpr uint32_t CheckPoint::Size() {
    return sizeof(transaction_id_t) +
           sizeof(log_sequence_number_t) +
           sizeof(off_t) +
           sizeof(uint32_t);
  }

  ostream & operator<<(ostream &stream, const LogEntry &logEntry){
    stream << "Log Entry:" << std::endl;
    stream << "Transaction ID: " << logEntry.transactionId << " "
          << " Log Sequence Number: " << logEntry.logSequenceNumber << std::endl;

    return logEntry.body->Print(stream);
  }


}
