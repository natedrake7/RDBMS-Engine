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

    this->logFileDescriptor = ::open(logFileName.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);

    this->checkPointFileDescriptor = ::open(logCheckPointFileName.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);

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

    constexpr size_t size = sizeof(CheckPoint) - sizeof(checkpoint.checkSum);
    uint32_t sum = 0;

    for (size_t i = 0; i < size; ++i)
        sum += data[i];

    return sum;
  }

int Transaction::GetSize()const{
    int size = this->GetStaticDataSize();

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

    buffer->resize(transaction.GetSize());

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
    const auto* table = tables.at(transaction.tableOrdinalPosition);

    memcpy(&transaction.hasOldRow, buffer.data() + pos, sizeof(bool));
    pos += sizeof(bool);
    Logger::DeserializeRow(buffer, pos, table);

    memcpy(&transaction.hasNewRow, buffer.data() + pos, sizeof(bool));
    pos += sizeof(bool);
    Logger::DeserializeRow(buffer, pos, table);
  }

 void Logger::SerializeRow(std::vector<char> *buffer, uint32_t& pos, StorageTypes::Row *row){
    const bool isRowNull = row == nullptr;

    memcpy(buffer->data() + pos, &isRowNull, sizeof(bool));
    pos += sizeof(bool);

    if (isRowNull)
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

    const __off_t fileOffset = lseek(this->logFileDescriptor, 0, SEEK_CUR);

    const auto result = ::write(this->logFileDescriptor, buffer.data(), buffer.size());

    this->FlushLogDescriptor();

    if (result < 0 || result != buffer.size()) {
      std::cerr << "Failed to write transaction to log file" << std::endl;
    }

    return {
      .transactionId = transaction.transactionId,
      .logSequenceNumber =  transaction.logSequenceNumber,
      .logFileOffset = fileOffset,
    };
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
      .transactionId = transactionId,
      .operation = operation,
      .tableOrdinalPosition = tableOrdinalPosition,
      .pageId = pageId,
      .rowIndex = rowIndex,
      .hasOldRow = oldRow == nullptr,
      .oldRow = oldRow,
      .hasNewRow = newRow == nullptr,
      .newRow = newRow,
      .logSequenceNumber = logSequenceNumber
    };
  }

  Constants::transaction_id_t Logger::StartTransaction(){
    std::unique_lock<std::mutex> lock(this->transactionLogMutex);

    this->transactionLogSequenceNumbers.Add(this->currentTransactionId, 0);

    return this->currentTransactionId++;
  }

  void Logger::LogCheckPoint(CheckPoint& checkPoint)const{
    checkPoint.checkSum = CheckPoint::CalculateCheckSum(checkPoint);

    const auto result = ::write(this->checkPointFileDescriptor, &checkPoint, sizeof(CheckPoint));

    if (result < 0 || result != sizeof(CheckPoint)) {
      std::cerr << "Failed to write checkpoint to checkpoint file" << std::endl;
    }
  }

  void Logger::RecoverLogs(const std::vector<StorageTypes::Table*>& tables){

    constexpr size_t BATCH_SIZE = 1024 * 1024; // 1 MB

    const auto lastValidCheckPoint = this->RecoverLastCheckPoint();

    ::lseek(this->logFileDescriptor, lastValidCheckPoint.logFileOffset, SEEK_SET);

    std::vector<char> buffer(BATCH_SIZE);
    std::vector<char> leftovers;
    while (::read(this->logFileDescriptor, buffer.data(), BATCH_SIZE) > 0) {
      buffer.insert(buffer.begin(), leftovers.begin(), leftovers.end());

      uint32_t pos = 0;
      std::vector<Transaction> transactions;

      while (pos < buffer.size()) {

        Transaction transaction;
        if (pos + transaction.GetStaticDataSize() > buffer.size()) {
          leftovers.assign(buffer.begin() + pos, buffer.end());
          break;
        }

        Logger::DeserializeTransactionHeader(buffer, transaction, pos);

        // Logger::DeserializeTransactionBody(buffer, transaction, tables, pos);

        if (transaction.transactionId == INVALID_TRANSACTION_ID) {
          std::cerr << "Invalid transaction ID encountered during recovery" << std::endl;
          continue;
        }

        transactions.push_back(transaction);

        // Process the transaction
        // This is where you would apply the transaction to the database state
        // For example:
        // this->ApplyTransaction(transaction);
        this->currentTransactionId = transaction.transactionId++;
      }

    }



  }

  CheckPoint Logger::RecoverLastCheckPoint() const{
    CheckPoint checkPoint{};
    ::lseek(this->checkPointFileDescriptor, sizeof(CheckPoint), SEEK_END);

    const auto result = ::read(this->checkPointFileDescriptor, &checkPoint, sizeof(CheckPoint));

    if (result < 0 || result != sizeof(CheckPoint)) {
      std::cerr << "Failed to read checkpoint from checkpoint file" << std::endl;
      throw std::runtime_error("Failed to recover last checkpoint");
    }

    const auto checkSum = CheckPoint::CalculateCheckSum(checkPoint);

    if (checkSum != checkPoint.checkSum) {
      std::cerr << "Checkpoint checksum mismatch" << std::endl;
      throw std::runtime_error("Checkpoint checksum mismatch");
    }

    return checkPoint;
  }
}
