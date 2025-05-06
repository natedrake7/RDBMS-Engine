#include "ConnectionManager.h"

#include <atomic>
#include <cstring>
#include <iostream>
#include <ostream>
#include <stdexcept>
#include <fcntl.h>
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
    vector<epoll_event> events(this->parameters.numberOfConnections);

    while (isServerRunning) {
      const int eventCount = epoll_wait(this->parameters.epollFileDescriptor, events.data(), events.size(), 10);

      if (eventCount < 0) {
        cerr << "epoll_wait failed: " << strerror(errno) << endl;
        continue;
      }

      for (int i = 0;i < eventCount; i++) {
        cout << "hello" << endl;
        
        if (!(events[i].events & EPOLLIN))
          continue;

        cout << "event found" << endl;
        
        if (events[i].data.fd != this->parameters.serverSocket) {
          ConnectionManager::HandleClientConnection(events[i].data.fd);
          continue;
        }

        sockaddr_in clientAddress = {};
        socklen_t clientSize = sizeof(clientAddress);
  
        const int clientSocket = accept(this->parameters.serverSocket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);

        const int flags = fcntl(clientSocket, F_GETFL, 0);
        fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);

        cout << "hello from client" << endl;

        if (clientSocket < 0) {
          std::cerr << "Connection with client failed to establish" << endl;
        }
        
        epoll_event clientEvent{};
        clientEvent.events = EPOLLIN | EPOLLET; // Edge-triggered for efficiency
        clientEvent.data.fd = clientSocket;

        epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_ADD, clientSocket, &clientEvent);
        std::cout << "Accepted client: " << clientSocket << std::endl;
      }
    }

    cout << "Closing connections" << endl;

    this->CloseServerConnection(events);
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
      this->CloseServerConnection({});
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

    epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_ADD, sock, &event);
  }

  void ConnectionManager::CloseServerConnection(const vector<epoll_event>& events) const
  {
    for (const auto& event : events) {
      epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, event.data.fd, nullptr);
#ifdef _WIN32
      closesocket(event.data.fd);
#else
      close(event.data.fd);
#endif
    }
    
#ifdef _WIN32
    closesocket(this->parameters.epollFileDescriptor);
    WSACleanup();
#else
    close(this->parameters.epollFileDescriptor);
#endif
  }

  void ConnectionManager::CloseClientConnection(const int &clientSocket) const
  {
    epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, clientSocket, nullptr);

#ifdef _WIN32
    closesocket(clientSocket);
    WSACleanup();
#else
    close(clientSocket);
#endif
  }

  void ConnectionManager::HandleClientConnection(const int &clientSocket) const{
    vector<unsigned char> buffer(100);
    const ssize_t bytesRead = recv(clientSocket, buffer.data(), 100, 0);
    
    if (bytesRead > 0) {
      cout<< buffer.data() << endl;
      return;
    }
    if (bytesRead == 0) {
      this->CloseClientConnection(clientSocket);
      return;
    }

    perror("recv failed");
  }
} // Server