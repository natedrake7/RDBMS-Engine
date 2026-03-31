#include "../../include/Logger/CrashRecoveryLogger.h"

namespace CoreEngine::Logging {

  CrashRecoveryLogger::CrashRecoveryLogger(const std::string &logFilePath)
    : Logger(logFilePath){}

  CrashRecoveryLogger::~CrashRecoveryLogger()= default;

  std::vector<LogEntry> CrashRecoveryLogger::RecoverLogs(const std::vector<StorageTypes::Table *> &tables){
    //get logs
    return {};
  }

}
