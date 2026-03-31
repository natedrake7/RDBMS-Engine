#include "../../include/Logger/UndoLogger.h"
#include <cstring>
#include <fcntl.h>
#include <iostream>
#ifdef _WIN32
  #include <io.h>
#else
  #include <unistd.h>
#endif

namespace CoreEngine::Logging{
    UndoLogger::UndoLogger(const std::string& logFilePath): Logger(logFilePath) {
   const std::string logCheckPointFileName = logFilePath + ".meta";

    this->checkPointFileDescriptor = ::open(logCheckPointFileName.c_str(), O_RDWR | O_CREAT, 0644);

   if (this->checkPointFileDescriptor < 0)
     throw std::runtime_error("Failed to open or create checkpoint file: " + logCheckPointFileName);
 }

  UndoLogger::~UndoLogger() {
     if (this->checkPointFileDescriptor >= 0)
       ::close(this->checkPointFileDescriptor);
  }

  UndoLogger & UndoLogger::Get() {
    const auto& logFilePath = Constants::UNDO_LOG_FILE.Data();

    static UndoLogger instance(logFilePath);
    return instance;
  }

  void UndoLogger::LogCheckPoint(CheckPoint& checkPoint)const{
     checkPoint.checkSum = CheckPoint::CalculateCheckSum(checkPoint);

     const auto result = ::write(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

     if (result < 0 || result != CheckPoint::Size()) {
       std::cerr << "Failed to write checkpoint to checkpoint file" << std::endl;
       //exception or not?
     }
   }

  std::vector<LogEntry> UndoLogger::RecoverLogs(const std::vector<StorageTypes::Table *> &tables){
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
      const auto bytesRead = ::read(this->logFileDescriptor, buffer.data(), Constants::LOG_BATCH_SIZE);

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

        // logEntry.AllocateBody();
        //
        // if (logEntry.body == nullptr)
        //   continue;
        //
        // logEntry.body->Deserialize(&buffer, pos, tables.at(logEntry.tableOrdinalPosition));

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

  void UndoLogger::FlushCheckPointDescriptor() const{
    #ifdef _WIN32
      _commit(this->logFileDescriptor);
    #else
      ::fsync(this->logFileDescriptor);
    #endif
  }

  CheckPoint UndoLogger::RecoverLastCheckPoint() const{
    CheckPoint checkPoint;

    const auto fileSize = lseek(this->checkPointFileDescriptor, 0, SEEK_END);
    if (fileSize < CheckPoint::Size()) {
      // File too small, no valid checkpoint
      return checkPoint;
    }

    ::lseek(this->checkPointFileDescriptor, static_cast<long>(fileSize - CheckPoint::Size()), SEEK_SET);

    // ::lseek(this->checkPointFileDescriptor, 0, SEEK_SET);
    const auto result = ::read(this->checkPointFileDescriptor, &checkPoint, CheckPoint::Size());

    if (result == 0) {
      //first insert failed no bytes were read
      checkPoint.transactionId = INVALID_TRANSACTION_ID;
      return checkPoint;
    }

    if (result < 0 || result != CheckPoint::Size()) {
      std::cerr << "Failed to read checkpoint from checkpoint file" << std::endl;
      // perror("Failed to read from checkpoint file");

      // throw std::runtime_error("Failed to recover last checkpoint");
    }

    // if (CheckPoint::CalculateCheckSum(checkPoint) != checkPoint.checkSum) {
    //   std::cerr << "Checkpoint checksum mismatch" << std::endl;
    //   throw std::runtime_error("Checkpoint checksum mismatch");
    // }

    return checkPoint;
  }
}