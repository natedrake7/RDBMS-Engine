#pragma once
#include "Logger.h"

namespace DatabaseEngine::Logging {
  class WriteAheadLogger final : public Logger{
    int checkPointFileDescriptor;

    void FlushCheckPointDescriptor()const;

      explicit WriteAheadLogger(const std::string& logFilePath);
      ~WriteAheadLogger()override;

    public:
      static WriteAheadLogger& Get();

      void LogCheckPoint(CheckPoint& checkPoint)const;
      std::vector<LogEntry>  RecoverLogs(const std::vector<StorageTypes::Table*>& tables)override;
      [[nodiscard]] CheckPoint RecoverLastCheckPoint() const;

  };
}
