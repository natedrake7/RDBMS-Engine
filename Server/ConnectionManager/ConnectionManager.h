#pragma once
#include <atomic>
#include <mutex>
#include <string>

#ifdef _WIN32

#define NOMINMAX
#define byte win_byte_override // Add this before any Windows headers

    #include <ws2tcpip.h>
    #include <windows.h>
    #include <winsock2.h>
    #pragma comment(lib, "ws2_32.lib")

#undef byte // Clean up after including

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

namespace Server {

  constexpr int MAX_CONNECTIONS = 10;

  typedef struct ConnectionParameters {
    int port;
    int serverSocket;
    int epollFileDescriptor;
    std::string hostName;

    int numberOfConnections;
    int timeoutTime;

    ConnectionParameters();
    explicit ConnectionParameters(const std::string& hostname, const int& port, const int& numberOfConnections, const int& timeoutTime);
  }ConnectionParameters;



  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const std::atomic<bool>& isServerRunning);

  class ConnectionManager {
    ConnectionParameters parameters;
    std::vector<SocketEvent> events;

    protected:
      static void AuthorizeClientConnection(const int& clientSocket, const ConnectionProtocolHeader &header, const std::vector<char>& buffer);
    
      void HandleClientConnection(const int& clientSocket, std::mutex& clientMutex) const;
      static void ReadBodyFromClient(const int& clientSocket, const ConnectionProtocolHeader& header);
    
      void CloseServerConnection() const;

      void CloseClientConnection(const int& clientSocket) const;
      void InitializeServerSocket();
    
    public:
      explicit ConnectionManager(const ConnectionParameters& parameters);
      ~ConnectionManager() = default;

      void HandleNewConnections(const std::atomic<bool>& isServerRunning);
  };



}