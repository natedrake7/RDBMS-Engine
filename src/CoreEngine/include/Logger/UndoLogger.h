#pragma once
#include "Logger.h"

namespace CoreEngine::Logging{
    class UndoLogger final : public Logger{
        int checkPointFileDescriptor;

        void FlushCheckPointDescriptor()const;

        explicit UndoLogger(const std::string& logFilePath);
        ~UndoLogger()override;

    public:
        static UndoLogger& Get();

        void LogCheckPoint(CheckPoint& checkPoint)const;
        std::vector<LogEntry> RecoverLogs(const std::vector<StorageTypes::Table*>& tables)override;
        [[nodiscard]] CheckPoint RecoverLastCheckPoint() const;
    };
}
