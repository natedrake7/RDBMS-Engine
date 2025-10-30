#pragma once
#include "../../Systemic/Network/Protocols/ConnectionProtocol/ResponseProtocol/ResponseProtocol.h"
#include "../Threadpool/ThreadPool.h"


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

#include "../../Systemic/Network/Protocols/ConnectionProtocol/ConnectionProtocol/ConnectionProtocol.h"

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
    ThreadPool threadPool;

    protected:
      static void SendToClient(const int& clientSocket, Network::ResponseProtocol* protocol);

      void GetQueryFromClient(const int& clientSocket, const Network::ConnectionProtocolHeader& header, const vector<char>& buffer);
      static void AuthorizeClientConnection(const int& clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char>& buffer);
      void HandleClientConnection(const int& clientSocket, mutex& clientMutex);
      void ReadBodyFromClient(const int& clientSocket, const Network::ConnectionProtocolHeader& header);
      void CloseServerConnection() const;
      static void CloseClientConnection(const int& clientSocket);
      void InitializeServerSocket();
    
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}