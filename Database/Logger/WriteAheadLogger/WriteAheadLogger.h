#pragma once
#include "../Logger.h"

namespace DatabaseEngine::Logging {
  class WriteAheadLogger : public Logger{
    int checkPointFileDescriptor;

    void FlushCheckPointDescriptor()const;
    [[nodiscard]] CheckPoint RecoverLastCheckPoint() const;

    public:
      explicit WriteAheadLogger(const std::string& logFilePath);
      ~WriteAheadLogger()override;

      void LogCheckPoint(CheckPoint& checkPoint)const;
      std::vector<LogEntry>  RecoverLogs(const std::vector<StorageTypes::Table*>& tables)override;
  };
}
