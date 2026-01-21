#pragma once
#include "../../Systemic/include/Network/ResponseProtocol.h"
#include "../../Systemic/include/Network/ConnectionProtocol.h"
#include "ThreadPool.h"


#include <atomic>
#include <string>

#ifdef _WIN32
#define NOMINMAX
#define byte win_byte_override // Add this before any Windows headers
    #include <ws2tcpip.h>
    #include <windows.h>
    #include <winsock2.h>
    #pragma comment(lib, "ws2_32.lib")
    using SocketEvent = pollfd;

#undef byte // Clean up after including
#else
    #include <sys/socket.h>
    #include <sys/epoll.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <sys/epoll.h>
    using SocketEvent = epoll_event;
#endif


using namespace std;

namespace Network {

  struct ConnectionParameters {
    Int port;
    Int serverSocket;
    Int epollFileDescriptor;
    std::string hostName;

    Int numberOfConnections;
    Int timeoutTime;

    ConnectionParameters();
    ConnectionParameters(const std::string& hostname, Int port, Int numberOfConnections, Int timeoutTime);
  };



  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning);

  class ConnectionManager {
    ConnectionParameters parameters;
    vector<SocketEvent> events;
    ThreadPool threadPool;

    protected:
      static void SendToClient(Int clientSocket, Network::ResponseProtocol* protocol);

      void GetQueryFromClient(Int clientSocket, const Network::ConnectionProtocolHeader& header, const vector<char>& buffer);
      void AuthorizeClientConnection(Int clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char>& buffer)const;
      void HandleClientConnection(Int clientSocket, mutex& clientMutex);
      void ReadBodyFromClient(Int clientSocket, const Network::ConnectionProtocolHeader& header);
      void CloseServerConnection() const;
      void CloseClientConnection(Int clientSocket) const;
      void InitializeServerSocket();

      static void ExecuteQuery(const std::string& query, Int socket, const Network::ConnectionProtocolHeader &header);

#ifdef _WIN32
      void HandleClientDisconnection(const SocketEvent& event, Int& totalEvents, Int& index);
#else
    void HandleClientDisconnection(Int socket, Int& totalEvents, Int& index);
#endif
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}