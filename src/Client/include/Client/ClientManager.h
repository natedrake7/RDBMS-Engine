#pragma once
#include <Client/Client.h>
#include <Systemic/DataTypes/Guid.h>

#include <string>
#include <vector>

#include <Systemic/Network/Header.h>

namespace Client {
    class ConnectionManager {
        ConnectionParameters _parameters;
        UInt32_t _nextRequestId;
        bool _networkInitialized;

        [[nodiscard]] UInt32_t NextRequestId(){
            return this->_nextRequestId++;
        }

        [[nodiscard]] bool InitializeConnectionToServer();
        void CloseConnectionToServer();
        [[nodiscard]] bool AuthenticateConnectionToServer();

        [[nodiscard]] bool ReadConnectionString(const std::vector<std::string>& connectionString);

        [[nodiscard]] bool SendFrame(
            Network::MessageType type,
            UInt32_t requestId,
            const char* payload,
            UInt32_t payloadLength
        )const;

        [[nodiscard]] bool ReadFrame(Network::Header* header, std::vector<char>* payload)const;

        [[nodiscard]] bool ReadQueryResponse(UInt32_t requestId)const;

        public:
            ConnectionManager() = default;
            ~ConnectionManager();

            ConnectionManager(const ConnectionManager&) = delete;
            ConnectionManager& operator=(const ConnectionManager&) = delete;


            [[nodiscard]] bool ConnectToServer(const std::vector<std::string>& connectionString);
            [[nodiscard]] bool ExecuteQuery(const std::string& query);
    };
}
