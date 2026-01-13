#pragma once
#include <string>

void shutdownClient(int signal);
void RegisterSignalHandlers();
void ExecuteQuery(const std::string& query, const DataTypes::Guid& sessionId);

inline std::atomic serverRunning{false};
inline std::array<std::thread, 3> backgroundThreads;