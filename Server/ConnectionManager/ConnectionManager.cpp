#include "ConnectionManager.h"

#include <atomic>
#include <iostream>
#include <ostream>
#include <stdexcept>
#ifdef _WIN32
  #include <winsock2.h>
  #pragma comment(lib, "ws2_32.lib")
#else
  #include <sys/socket.h>
  #include <sys/epoll.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

namespace Server {

  void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning)
  {
    ConnectionManager _connectionManager(parameters);

    _connectionManager.HandleNewConnections(isServerRunning);
  }

  ConnectionManager::ConnectionManager(const ConnectionParameters &parameters)
  {
    this->parameters = parameters;
  }

  void ConnectionManager::HandleNewConnections(const atomic<bool>& isServerRunning)
  {

    this->InitializeServerSocket();

    
    while (isServerRunning) {
      int eventCount = epoll_wait(this->parameters.epollFileDescriptor, this->events.data(), this->events.size(), -1);
      if (eventCount < 0) {
        perror("epoll_wait");
        continue;
      }

      for (int i = 0;i < eventCount; i++) {
        if (events[i].data.fd == this->parameters.serverSocket) {
          sockaddr_in clientAddress = {};
          socklen_t clientSize = sizeof(clientAddress);
    
          const int clientSocket = accept(this->parameters.serverSocket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);

          if (clientSocket < 0) {
            std::cerr << "Connection with client failed to establish" << endl;
          }
          
          epoll_event clientEvent{};
          clientEvent.events = EPOLLIN | EPOLLET; // Edge-triggered for efficiency
          clientEvent.data.fd = clientSocket;

          epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_ADD, clientSocket, &clientEvent);
          std::cout << "Accepted client: " << clientSocket << std::endl;
        }
        else {
          //client sockets
        }
      }
      

    }

    cout << "Closing connections" << endl;

    ConnectionManager::CloseServerConnection(parameters);
  }

  void ConnectionManager::InitializeServerSocket()
  {
            
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
      throw runtime_error( "WSAStartup failed");
#endif
    
    const int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0)
      throw runtime_error("Failed to create socket");

    sockaddr_in serverAddress = {};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = inet_addr(this->parameters.hostName.c_str());
    serverAddress.sin_port = htons(this->parameters.port);

    if (bind(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0)
      throw runtime_error("Failed to bind socket");
    
    if (listen(sock, SOMAXCONN) < 0) {
      CloseServerConnection(this->parameters);
      throw runtime_error("Failed to listen on socket");
    }

    const int epollFd = epoll_create1(0);
    if (epollFd == -1)
      throw std::runtime_error("Failed to create epoll file descriptor");

    this->parameters.epollFileDescriptor = epollFd;
    this->parameters.serverSocket = sock;

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = sock;

    this->events.push_back(event);
  }

  void ConnectionManager::CloseServerConnection(const ConnectionParameters &parameters)
  {
    #ifdef _WIN32
        closesocket(parameters.serverSocket);
        WSACleanup();
    #else
        close(parameters.serverSocket);
    #endif
  }

  void ConnectionManager::CloseClientConnection(const ConnectionParameters &parameters)
  {
    // #ifdef _WIN32
    //     closesocket(parameters.clientSocket);
    //     WSACleanup();
    // #else
    //     close(parameters.clientSocket);
    // #endif
  }
} // Server