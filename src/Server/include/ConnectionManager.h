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
    int port;
    int serverSocket;
    int epollFileDescriptor;
    string hostName;

    int numberOfConnections;
    int timeoutTime;

    ConnectionParameters();
    ConnectionParameters(const string& hostname, const int& port, const int& numberOfConnections, const int& timeoutTime);
  };



  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning);

  class ConnectionManager {
    ConnectionParameters parameters;
    vector<SocketEvent> events;
    ThreadPool threadPool;

    protected:
      static void SendToClient(const int& clientSocket, Network::ResponseProtocol* protocol);

      void GetQueryFromClient(const int& clientSocket, const Network::ConnectionProtocolHeader& header, const vector<char>& buffer);
      void AuthorizeClientConnection(const int& clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char>& buffer)const;
      void HandleClientConnection(const int& clientSocket, mutex& clientMutex);
      void ReadBodyFromClient(const int& clientSocket, const Network::ConnectionProtocolHeader& header);
      void CloseServerConnection() const;
      void CloseClientConnection(const int& clientSocket) const;
      void InitializeServerSocket();

      static void ExecuteQuery(const std::string& query, const int& socket, const Network::ConnectionProtocolHeader &header);

#ifdef _WIN32
      void HandleClientDisconnection(const SocketEvent& event, int& totalEvents, int& index);
#else
    void HandleClientDisconnection(const int& socket, int& totalEvents, int& index);
#endif
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}