#pragma once
#include <string>
#include <atomic>

namespace DataTypes{
    class Guid;
}

namespace Network{
    class Server;
}

#ifndef NDEBUG
    #define IS_DEBUG
#endif

void shutdownClient(int signal);
void RegisterSignalHandlers();
void ExecuteQuery(const std::string& query, const DataTypes::Guid& sessionId);
void CommandLineInterface(Network::Server& server);

inline std::atomic serverRunning{false};