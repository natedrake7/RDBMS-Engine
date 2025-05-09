#pragma once
#include <atomic>
#include <string>
#include <sys/epoll.h>

#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.h"

using namespace std;

namespace Server {

  constexpr int MAX_CONNECTIONS = 10;

  typedef struct ConnectionParameters {
    int port;
    int serverSocket;
    int epollFileDescriptor;
    string hostName;

    int numberOfConnections;
    int timeoutTime;

    ConnectionParameters();
    explicit ConnectionParameters(const string& hostname, const int& port, const int& numberOfConnections, const int& timeoutTime);
  }ConnectionParameters;



  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning);

  class ConnectionManager {
    ConnectionParameters parameters;

    protected:
      static void AuthorizeClientConnection(const int& clientSocket, const ConnectionProtocolHeader &header, const vector<unsigned char>& buffer);
    
      void HandleClientConnection(const int& clientSocket, mutex& clientMutex) const;
      static void ReadBodyFromClient(const int& clientSocket, const ConnectionProtocolHeader& header);
    
      void CloseServerConnection(const vector<epoll_event>& events) const;
      void CloseClientConnection(const int& clientSocket) const;
      void InitializeServerSocket();
    
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}