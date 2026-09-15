#pragma once
#include "../include/Client.h"
#include "../../Systemic/include/DataTypes/Guid.h"

#include <string>
#include <vector>

#include "Network/Header.h"

namespace Client {
    class ConnectionManager {
        ConnectionParameters _parameters;
        UnsignedInt _nextRequestId;
        bool _networkInitialized;

        [[nodiscard]] UnsignedInt NextRequestId(){
            return this->_nextRequestId++;
        }

        [[nodiscard]] bool InitializeConnectionToServer();
        void CloseConnectionToServer();
        [[nodiscard]] bool AuthenticateConnectionToServer();

        [[nodiscard]] bool ReadConnectionString(const std::vector<std::string>& connectionString);

        [[nodiscard]] bool SendFrame(
            Network::MessageType type,
            UnsignedInt requestId,
            const char* payload,
            UnsignedInt payloadLength
        )const;

        [[nodiscard]] bool ReadFrame(Network::Header* header, std::vector<char>* payload)const;

        [[nodiscard]] bool ReadQueryResponse(UnsignedInt requestId)const;

        public:
            ConnectionManager() = default;
            ~ConnectionManager();

            ConnectionManager(const ConnectionManager&) = delete;
            ConnectionManager& operator=(const ConnectionManager&) = delete;


            [[nodiscard]] bool ConnectToServer(const std::vector<std::string>& connectionString);
            [[nodiscard]] bool ExecuteQuery(const std::string& query);
    };
}
