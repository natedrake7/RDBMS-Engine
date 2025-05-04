#pragma once
#include <atomic>
#include <string>

using namespace std;

namespace Server {

  typedef struct ConnectionParameters {
    int port;
    string hostName;

    int socket;
    int clientSocket;
  }ConnectionParameters;

  void HandleNewConnections(ConnectionParameters& parameters, const atomic<bool>& isServerRunning);
  void CloseServerConnection(const ConnectionParameters& parameters);
  void CloseClientConnection(const ConnectionParameters& parameters);

}