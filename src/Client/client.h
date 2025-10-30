#pragma once

typedef struct ConnectionParameters {
  int port;
  string hostName;
  string username;
  string password;

  int socket;

  ConnectionParameters() {
    this->port = 0;
    this->socket = 0;
  }
  ~ConnectionParameters() = default;
}ConnectionParameters;

static void ValidateConnectionString(ConnectionParameters& params, const vector<string>& connectionString);
void InitializeConnectionToServer(ConnectionParameters& params);
void CloseConnection(ConnectionParameters& params);
