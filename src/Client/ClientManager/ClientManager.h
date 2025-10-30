#pragma once
#include "../client.h"
#include "../../Systemic/DataTypes/Guid/Guid.h"

#include <string>
#include <vector>

namespace Client {
  class ConnectionManager {
    ConnectionParameters parameters;
    DataTypes::Guid sessionId;

    [[nodiscard]] bool ReadConnectionString(const std::vector<std::string>& connectionString);
    [[nodiscard]] bool InitializeConnectionToServer();
    [[nodiscard]] bool AuthenticateConnectionToServer();
    void CloseConnectionToServer();

    public:
      ConnectionManager();
      ~ConnectionManager();


    [[nodiscard]] bool ConnectToServer(const std::vector<std::string>& connectionString);
    [[nodiscard]] bool SendQuery(const std::string& query)const;
    [[nodiscard]] bool ParseQueryResponse()const;
  };
}
