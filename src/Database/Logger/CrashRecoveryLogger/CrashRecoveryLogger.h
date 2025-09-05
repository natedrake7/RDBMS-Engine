#pragma once
#include "../Logger.h"

namespace DatabaseEngine::Logging {
 class CrashRecoveryLogger final : public Logger{

  public:
   explicit CrashRecoveryLogger(const std::string& logFilePath);
  ~CrashRecoveryLogger()override;

  std::vector<LogEntry>  RecoverLogs(const std::vector<StorageTypes::Table*>& tables)override;
 };

}

