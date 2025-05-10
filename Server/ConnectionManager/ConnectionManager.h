#pragma once
#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.h"


#include <atomic>
#include <string>

#ifdef _WIN32
    #include <ws2tcpip.h>
    #include <windows.h>
    #include <winsock2.h>
    #pragma comment(lib, "ws2_32.lib")
    using SocketEvent = pollfd;
#else
    #include <sys/socket.h>
    #include <sys/epoll.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <sys/epoll.h>
    using SocketEvent = epoll_event;
#endif

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
    vector<SocketEvent> events;

    protected:
      static void SendToClient(const int& clientSocket, ResponseProtocol* protocol);

      static void GetQueryFromClient(const int& clientSocket, const ConnectionProtocolHeader& header, const vector<unsigned char>& buffer);
      void AuthorizeClientConnection(const int& clientSocket, const ConnectionProtocolHeader &header, const vector<unsigned char>& buffer)const;
      void HandleClientConnection(const int& clientSocket, mutex& clientMutex) const;
      void ReadBodyFromClient(const int& clientSocket, const ConnectionProtocolHeader& header)const;
      void CloseServerConnection() const;
      void CloseClientConnection(const int& clientSocket) const;
      void InitializeServerSocket();
    
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}