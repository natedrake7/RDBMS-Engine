//
// Created by kolampropoulos on 7/10/25.
//

#include "Logger.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Table/Table.h"
#include "../Block/Block.h"

#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

namespace DatabaseEngine::Logging {
  Logger::Logger(const std::string& logFilePath){
    const std::string logFileName = logFilePath + ".wal";
    const std::string logCheckPointFileName = logFilePath + ".meta";

    this->logFileDescriptor = ::open(logFileName.c_str(), O_RDWR | O_CREAT, 0644);

    this->checkPointFileDescriptor = ::open(logCheckPointFileName.c_str(), O_RDWR | O_CREAT, 0644);

    if (this->logFileDescriptor < 0) {
      throw std::runtime_error("Failed to open or create log file: " + logFilePath);
    }

    if (this->checkPointFileDescriptor < 0) {
      throw std::runtime_error("Failed to open or create checkpoint file: " + logCheckPointFileName);
    }

    this->currentTransactionId = 0;
  }

  Logger::~Logger(){

    if (this->logFileDescriptor >= 0) {
      ::close(this->logFileDescriptor);
    }

    if (this->checkPointFileDescriptor >= 0) {
      ::close(this->checkPointFileDescriptor);
    }
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

 Transaction::Transaction() {
    this->transactionId = INVALID_TRANSACTION_ID;
    this->logSequenceNumber = 0;
    this->operation = OperationType::InvalidOperation;
    this->pageId = INVALID_PAGE_ID;
    this->rowIndex = 0;
    this->tableOrdinalPosition = 0;

    this->hasNewRow = false;
    this->oldRow = nullptr;

    this->hasOldRow = false;
    this->newRow = nullptr;
  }


  Transaction::Transaction(
    const Constants::transaction_id_t &transactionId,
    const Constants::log_sequence_number_t& logSequenceNumber,
    const OperationType &operation,
    const Constants::table_id_t &tableOrdinalPosition,
    const Constants::page_id_t &pageId,
    const int &rowIndex,
    StorageTypes::Row *oldRow,
    StorageTypes::Row *newRow){

    this->transactionId = transactionId;
    this->logSequenceNumber = logSequenceNumber;
    this->operation = operation;
    this->pageId = pageId;
    this->rowIndex = rowIndex;
    this->tableOrdinalPosition = tableOrdinalPosition;

    this->hasNewRow = oldRow == nullptr;
    this->oldRow = oldRow;

    this->hasOldRow = newRow == nullptr;
    this->newRow = newRow;
  }

int Transaction::GetSize()const{
    int size = this->GetStaticDataSize();

    size += sizeof(bool) * 2; // hasOldRow and hasNewRow
    size += this->oldRow == nullptr ? 0 : static_cast<int>(this->oldRow->GetRowSize());
    size += this->newRow == nullptr ? 0 : static_cast<int>(this->newRow->GetRowSize());

    return size;
  }

  int Transaction::GetStaticDataSize()const {
    return
        sizeof(this->transactionId) +
        sizeof(this->logSequenceNumber) +
        sizeof(this->operation) +
        sizeof(this->tableOrdinalPosition) +
        sizeof(this->pageId) +
        sizeof(this->rowIndex);
  }

  void Logger::FlushLogDescriptor()const{
      fsync(this->logFileDescriptor);
    }

  void Logger::FlushCheckPointDescriptor() const{
    fsync(this->checkPointFileDescriptor);
  }

void Logger::SerializeTransaction(std::vector<char> *buffer, const Transaction &transaction) {
    uint32_t pos = 0;

    const auto transactionSize = transaction.GetSize();

    buffer->resize(transactionSize + sizeof(transactionSize));

    memcpy(buffer->data() + pos, &transactionSize, sizeof(transactionSize));
    pos += sizeof(transactionSize);
    memcpy(buffer->data() + pos, &transaction.transactionId, sizeof(transaction.transactionId));
    pos += sizeof(transaction.transactionId);
    memcpy(buffer->data() + pos, &transaction.logSequenceNumber, sizeof(transaction.logSequenceNumber));
    pos += sizeof(transaction.logSequenceNumber);
    memcpy(buffer->data() + pos, &transaction.operation, sizeof(transaction.operation));
    pos += sizeof(transaction.operation);
    memcpy(buffer->data() + pos, &transaction.tableOrdinalPosition, sizeof(transaction.tableOrdinalPosition));
    pos += sizeof(transaction.tableOrdinalPosition);
    memcpy(buffer->data() + pos, &transaction.pageId, sizeof(transaction.pageId));
    pos += sizeof(transaction.pageId);
    memcpy(buffer->data() + pos, &transaction.rowIndex, sizeof(transaction.rowIndex));
    pos += sizeof(transaction.rowIndex);

    Logger::SerializeRow(buffer, pos, transaction.oldRow);

    Logger::SerializeRow(buffer, pos, transaction.newRow);

  }

  void Logger::DeserializeTransactionHeader(const std::vector<char> &buffer, Transaction &transaction, uint32_t &pos){
    memcpy(&transaction.transactionId, buffer.data() + pos, sizeof(transaction.transactionId));
    pos += sizeof(transaction.transactionId);

    memcpy(&transaction.logSequenceNumber, buffer.data() + pos, sizeof(transaction.logSequenceNumber));
    pos += sizeof(transaction.logSequenceNumber);

    memcpy(&transaction.operation, buffer.data() + pos, sizeof(transaction.operation));
    pos += sizeof(transaction.operation);

    memcpy(&transaction.tableOrdinalPosition, buffer.data() + pos, sizeof(transaction.tableOrdinalPosition));
    pos += sizeof(transaction.tableOrdinalPosition);

    memcpy(&transaction.pageId, buffer.data() + pos, sizeof(transaction.pageId));
    pos += sizeof(transaction.pageId);

    memcpy(&transaction.rowIndex, buffer.data() + pos, sizeof(transaction.rowIndex));
    pos += sizeof(transaction.rowIndex);
  }

  //TODO implement to allow row-splitting between batches
  void Logger::DeserializeTransactionBody(
    const std::vector<char> &buffer,
    Transaction &transaction,
    const std::vector<StorageTypes::Table*> &tables,
    uint32_t& pos){
    // const auto* table = tables.at(transaction.tableOrdinalPosition);

    memcpy(&transaction.hasOldRow, buffer.data() + pos, sizeof(bool));
    pos += sizeof(bool);

    if (transaction.hasOldRow)
      Logger::DeserializeRow(buffer, pos, nullptr);

    memcpy(&transaction.hasNewRow, buffer.data() + pos, sizeof(bool));
    pos += sizeof(bool);

    if (transaction.hasNewRow)
      Logger::DeserializeRow(buffer, pos, nullptr);
  }

 void Logger::SerializeRow(std::vector<char> *buffer, uint32_t& pos, StorageTypes::Row *row){
    const bool hasRow = row != nullptr;

    memcpy(buffer->data() + pos, &hasRow, sizeof(bool));
    pos += sizeof(bool);

    if (!hasRow)
      return;

    const StorageTypes::RowHeader *rowHeader = row->GetHeader();

    memcpy(buffer->data() + pos, &rowHeader->rowSize, sizeof(row_size_t));
    pos += sizeof(row_size_t);
    memcpy(buffer->data() + pos, &rowHeader->maxRowSize, sizeof(size_t));
    pos += sizeof(size_t);

    rowHeader->nullBitMap->WriteDataToFile(buffer, pos);
    rowHeader->largeObjectBitMap->WriteDataToFile(buffer, pos);
    rowHeader->overflowBitMap->WriteDataToFile(buffer, pos);

    column_index_t columnIndex = 0;
    for (const auto &block : row->GetData())
    {
      if (rowHeader->nullBitMap->Get(columnIndex))
      {
        columnIndex++;
        continue;
      }

      block_size_t dataSize = block->GetBlockSize();

      memcpy(buffer->data() + pos, &dataSize, sizeof(block_size_t));
      pos += sizeof(block_size_t);


      const auto &blockData = block->GetBlockData();

      memcpy(buffer->data() + pos, blockData, dataSize);

      columnIndex++;
    }
  }

  StorageTypes::Row* Logger::DeserializeRow(const std::vector<char> &buffer, uint32_t &pos, const StorageTypes::Table* table){
    auto* row = new StorageTypes::Row(*table);

    auto *rowHeader = row->GetHeader();

    memcpy(&rowHeader->rowSize, buffer.data() + pos, sizeof(row_size_t));
    pos += sizeof(row_size_t);

    memcpy(&rowHeader->maxRowSize, buffer.data() + pos, sizeof(size_t));
    pos += sizeof(size_t);

    rowHeader->nullBitMap->GetDataFromFile(buffer, pos);
    rowHeader->largeObjectBitMap->GetDataFromFile(buffer, pos);
    rowHeader->overflowBitMap->GetDataFromFile(buffer, pos);

    const auto& columns = table->GetColumns();

    for (int j = 0; j < columns.size(); j++)
    {
      if (rowHeader->nullBitMap->Get(j))
      {
        auto *block = new StorageTypes::Block(nullptr, 0, columns[j]);

        row->InsertColumnData(block, j);

        continue;
      }

      block_size_t bytesToRead;

      memcpy(&bytesToRead, buffer.data() + pos, sizeof(block_size_t));
      pos += sizeof(block_size_t);

      auto *bytes = new unsigned char[bytesToRead];
      memcpy(bytes, buffer.data() + pos, bytesToRead);

      pos += bytesToRead;

      auto *block = new StorageTypes::Block(bytes, bytesToRead, columns[j]);

      row->InsertColumnData(block, j);

      delete[] bytes;
    }

    return row;
  }

  CheckPoint Logger::Log(const Transaction &transaction)const{
    std::vector<char> buffer;
    Logger::SerializeTransaction(&buffer, transaction);

    const __off_t fileOffset = lseek(this->logFileDescriptor, 0, SEEK_END);

    const auto result = ::write(this->logFileDescriptor, buffer.data(), buffer.size());

    this->FlushLogDescriptor();

    if (result < 0 || result != buffer.size()) {
      std::cerr << "Failed to write transaction to log file" << std::endl;
    }

    return { transaction.transactionId, transaction.logSequenceNumber, fileOffset };
  }

  Transaction Logger::CreateTransaction(
    const Constants::transaction_id_t& transactionId,
    const OperationType &operation,
    const Constants::table_id_t& tableOrdinalPosition,
    const Constants::page_id_t& pageId,
    const int& rowIndex,
    StorageTypes::Row *oldRow,
    StorageTypes::Row *newRow) {

    const auto logSequenceNumber = this->transactionLogSequenceNumbers.Get(transactionId);
    this->transactionLogSequenceNumbers.Update(transactionId, logSequenceNumber + 1);

    return {
      transactionId,
      logSequenceNumber,
      operation,
      tableOrdinalPosition,
      pageId,
      rowIndex,
      oldRow,
      newRow,
    };
  }

  Constants::transaction_id_t Logger::StartTransaction(){
    std::unique_lock<std::mutex> lock(this->transactionLogMutex);

    this->transactionLogSequenceNumbers.Add(this->currentTransactionId, 0);

    return this->currentTransactionId++;
  }

  void Logger::LogCheckPoint(CheckPoint& checkPoint)const{
    checkPoint.checkSum = CheckPoint::CalculateCheckSum(checkPoint);

    const auto result = ::write(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

    if (result < 0 || result != CheckPoint::Size()) {
      std::cerr << "Failed to write checkpoint to checkpoint file" << std::endl;
    }
  }

  void Logger::RecoverLogs(const std::vector<StorageTypes::Table*>& tables){

    constexpr size_t BATCH_SIZE = 1024 * 1024; // 1 MB

    const auto lastValidCheckPoint = this->RecoverLastCheckPoint();

    this->SetCurrentTransactionId(lastValidCheckPoint.transactionId + 1);

    if (lastValidCheckPoint.transactionId == INVALID_TRANSACTION_ID) {
      std::cerr << "No valid checkpoint found, recovery cannot proceed." << std::endl;
      return;
    }

    ::lseek(this->logFileDescriptor, lastValidCheckPoint.logFileOffset, SEEK_SET);

    std::vector<char> buffer(BATCH_SIZE);
    std::vector<char> leftovers;

    std::vector<Transaction> transactions;

    int transactionSize = 0;

    while (true) {
      const auto bytesRead = read(this->logFileDescriptor, buffer.data(), BATCH_SIZE);

      if (bytesRead < 0) {
        perror("Failed to read from log file");
        throw std::runtime_error("Failed to recover logs");
      }

      if (bytesRead == 0) {
        //TODO check if leftovers are available and process them
        return;
      }

      uint32_t pos = 0;
      buffer.insert(buffer.begin(), leftovers.begin(), leftovers.end());

      while (pos < bytesRead) {

        Transaction transaction;
        if (pos + transaction.GetStaticDataSize() + sizeof(int) > buffer.size()) {
          leftovers.assign(buffer.begin() + pos, buffer.end());
          break;
        }

        // ReSharper disable once CppDFAConstantConditions
        if (transaction.transactionId == INVALID_TRANSACTION_ID) {
            memcpy(&transactionSize, buffer.data() + pos, sizeof(transactionSize));
            pos += sizeof(transactionSize);

            Logger::DeserializeTransactionHeader(buffer, transaction, pos);
        }

        if (pos + transactionSize - transaction.GetStaticDataSize() > buffer.size()) {
          leftovers.assign(buffer.begin() + pos, buffer.end());
          break;
        }

        Logger::DeserializeTransactionBody(buffer, transaction, tables, pos);

        // ReSharper disable once CppDFAConstantConditions
        if (transaction.transactionId == INVALID_TRANSACTION_ID) {
          std::cerr << "Invalid transaction ID encountered during recovery" << std::endl;
          continue;
        }

        if (transaction.transactionId == lastValidCheckPoint.transactionId
          && transaction.logSequenceNumber == lastValidCheckPoint.logSequenceNumber)
          continue;

        transactions.push_back(transaction);

        leftovers.clear();
        buffer.clear();

        // Process the transaction
        // This is where you would apply the transaction to the database state
        // For example:
        // this->ApplyTransaction(transaction);
        // this->currentTransactionId = transaction.transactionId++;
      }
    }
  }

//TODO check how to read last checkpoint
  CheckPoint Logger::RecoverLastCheckPoint() const{
    CheckPoint checkPoint;

    const auto fileSize = lseek(this->checkPointFileDescriptor, 0, SEEK_END);
    if (fileSize < CheckPoint::Size()) {
      // File too small, no valid checkpoint
      return checkPoint;
    }

    ::lseek(this->checkPointFileDescriptor, fileSize - CheckPoint::Size(), SEEK_SET);

    // ::lseek(this->checkPointFileDescriptor, 0, SEEK_SET);
    const auto result = ::read(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

    if (result == 0)
      return checkPoint;

    if (result < 0 || result != CheckPoint::Size()) {
      std::cerr << "Failed to read checkpoint from checkpoint file" << std::endl;
      perror("Failed to read from checkpoint file");

      throw std::runtime_error("Failed to recover last checkpoint");
    }

    // if (CheckPoint::CalculateCheckSum(checkPoint) != checkPoint.checkSum) {
    //   std::cerr << "Checkpoint checksum mismatch" << std::endl;
    //   throw std::runtime_error("Checkpoint checksum mismatch");
    // }

    return checkPoint;
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
}
