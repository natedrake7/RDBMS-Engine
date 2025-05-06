#pragma once
#include <atomic>
#include <queue>
#include <string>
#include <sys/epoll.h>

using namespace std;

namespace Server {

  constexpr int MAX_CONNECTIONS = 10;

  typedef struct ConnectionParameters {
    int port;
    int serverSocket;
    int epollFileDescriptor;
    string hostName;

    int numberOfConnections;

  }ConnectionParameters;

  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning);

  class ConnectionManager {
    ConnectionParameters parameters;

    protected:
      void CloseServerConnection(const vector<epoll_event>& events)const;
      void CloseClientConnection(const int& clientSocket)const;
      void HandleClientConnection(const int& clientSocket)const;
      void InitializeServerSocket();
    
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}