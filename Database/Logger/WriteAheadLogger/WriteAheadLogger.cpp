#include "WriteAheadLogger.h"

#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>


namespace DatabaseEngine::Logging {

WriteAheadLogger::WriteAheadLogger(const std::string& logFilePath): Logger(logFilePath) {
   const std::string logCheckPointFileName = logFilePath + ".meta";

    this->checkPointFileDescriptor = ::open(logCheckPointFileName.c_str(), O_RDWR | O_CREAT, 0644);

   if (this->checkPointFileDescriptor < 0)
     throw std::runtime_error("Failed to open or create checkpoint file: " + logCheckPointFileName);
 }

  WriteAheadLogger::~WriteAheadLogger() {
     if (this->checkPointFileDescriptor >= 0)
       ::close(this->checkPointFileDescriptor);
  }

  void WriteAheadLogger::LogCheckPoint(CheckPoint& checkPoint)const{
     checkPoint.checkSum = CheckPoint::CalculateCheckSum(checkPoint);

     const auto result = ::write(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

     if (result < 0 || result != CheckPoint::Size()) {
       std::cerr << "Failed to write checkpoint to checkpoint file" << std::endl;
       //exception or not?
     }
   }

  std::vector<LogEntry> WriteAheadLogger::RecoverLogs(const std::vector<StorageTypes::Table *> &tables){
    const auto lastValidCheckPoint = this->RecoverLastCheckPoint();

    this->SetCurrentTransactionId(lastValidCheckPoint.transactionId + 1);

    if (lastValidCheckPoint.transactionId == INVALID_TRANSACTION_ID) {
      std::cerr << "No valid checkpoint found, recovery cannot proceed." << std::endl;
      return {};
    }

    ::lseek(this->logFileDescriptor, lastValidCheckPoint.logFileOffset, SEEK_SET);

    std::vector<char> buffer(Constants::LOG_BATCH_SIZE);
    std::vector<char> leftovers;

    std::vector<LogEntry> logEntries;

    int entrySize = 0;

    while (true) {
      const auto bytesRead = read(this->logFileDescriptor, buffer.data(), Constants::LOG_BATCH_SIZE);

      if (bytesRead < 0) {
        perror("Failed to read from log file");
        throw std::runtime_error("Failed to recover logs");
      }

      if (bytesRead == 0) {
        //TODO check if leftovers are available and process them
        return logEntries;
      }

      uint32_t pos = 0;
      buffer.insert(buffer.begin(), leftovers.begin(), leftovers.end());

      while (pos < bytesRead) {

        LogEntry logEntry;
        if (pos + logEntry.GetStaticDataSize() + sizeof(int) > buffer.size()) {
          leftovers.assign(buffer.begin() + pos, buffer.end());
          break;
        }

        // ReSharper disable once CppDFAConstantConditions
        if (logEntry.transactionId == INVALID_TRANSACTION_ID) {
            memcpy(&entrySize, buffer.data() + pos, sizeof(entrySize));
            pos += sizeof(entrySize);

          logEntry.DeserializeHeader(buffer, pos);
        }

        if (pos + entrySize - logEntry.GetStaticDataSize() > buffer.size()) {
          leftovers.assign(buffer.begin() + pos, buffer.end());
          break;
        }

        logEntry.AllocateBody();

        if (logEntry.body == nullptr)
          continue;

        logEntry.body->Deserialize(&buffer, pos, tables.at(logEntry.tableOrdinalPosition));

        // ReSharper disable once CppDFAConstantConditions
        if (logEntry.transactionId == INVALID_TRANSACTION_ID) {
          std::cerr << "Invalid transaction ID encountered during recovery" << std::endl;
          continue;
        }

        std::cout << logEntry << std::endl;

        if (logEntry.transactionId == lastValidCheckPoint.transactionId
          && logEntry.logSequenceNumber == lastValidCheckPoint.logSequenceNumber)
          continue;

        logEntries.push_back(logEntry);

        leftovers.clear();
        buffer.clear();
      }
    }
  }

  void WriteAheadLogger::FlushCheckPointDescriptor() const{
    fsync(this->checkPointFileDescriptor);
  }

  CheckPoint WriteAheadLogger::RecoverLastCheckPoint() const{
    CheckPoint checkPoint;

    const auto fileSize = lseek(this->checkPointFileDescriptor, 0, SEEK_END);
    if (fileSize < CheckPoint::Size()) {
      // File too small, no valid checkpoint
      return checkPoint;
    }

    ::lseek(this->checkPointFileDescriptor, fileSize - CheckPoint::Size(), SEEK_SET);

    // ::lseek(this->checkPointFileDescriptor, 0, SEEK_SET);
    const auto result = ::read(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

    if (result == 0) {
      //first insert failed no bytes were read
      checkPoint.transactionId = 0;
      return checkPoint;
    }

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

}