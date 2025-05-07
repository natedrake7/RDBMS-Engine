#include "ConnectionManager.h"

#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol.h"

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

  ConnectionParameters::ConnectionParameters() {
    this->hostName = "127.0.0.1";
    this->numberOfConnections = 20;
    this->timeoutTime = 10;
    this->port = 1433;

    this->epollFileDescriptor = -1;
    this->serverSocket = -1;
  }

  ConnectionParameters::ConnectionParameters(const string& hostname, const int& port, const int& numberOfConnections, const int& timeoutTime){
    this->hostName = hostname;
    this->numberOfConnections = numberOfConnections;
    this->timeoutTime = timeoutTime;
    this->port = port;

    this->epollFileDescriptor = -1;
    this->serverSocket = -1;
  }

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
    vector<mutex> eventMutexes(this->parameters.numberOfConnections);

    while (isServerRunning) {
      const int eventCount = epoll_wait(this->parameters.epollFileDescriptor, events.data(), events.size(), 10);

      if (eventCount < 0) {
        cerr << "epoll_wait failed: " << strerror(errno) << endl;
        continue;
      }

      for (int i = 0;i < eventCount; i++) {

        if (!eventMutexes[i].try_lock())
            continue;

        eventMutexes[i].unlock();
        
        if (!(events[i].events & EPOLLIN))
          continue;

        if (events[i].data.fd != this->parameters.serverSocket) {
          ConnectionManager::HandleClientConnection(events[i].data.fd, eventMutexes[i]);
          continue;
        }

        sockaddr_in clientAddress = {};
        socklen_t clientSize = sizeof(clientAddress);
  
        const int clientSocket = accept(this->parameters.serverSocket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);

        const int flags = fcntl(clientSocket, F_GETFL, 0);
        fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);

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

  void ConnectionManager::HandleClientConnection(const int &clientSocket, mutex& clientMutex) const{

    clientMutex.lock();
    
    ConnectionProtocol protocol;
    const ssize_t headerBytesRead = recv(clientSocket, &protocol.header, sizeof(ConnectionProtocolHeader), 0);

    if (headerBytesRead > 0) {
      ConnectionManager::ReadBodyFromClient(clientSocket, protocol);
      clientMutex.unlock();
      return;
    }
    
    if (headerBytesRead == 0) {
      this->CloseClientConnection(clientSocket);
      clientMutex.unlock();
      return;
    }
    
    clientMutex.unlock();
    perror("recv failed");
  }

  void ConnectionManager::ReadBodyFromClient(const int& clientSocket, ConnectionProtocol &protocol){
    protocol.buffer.resize(protocol.header.size);

    if (recv(clientSocket, protocol.buffer.data(), protocol.header.size, 0) <= 0) {
      perror("failed to read body from client or body was empty!");
      return;
    }

    if (protocol.header.dataType == ConnectionProtocolType::Authorize)
      ConnectionManager::AuthorizeClientConnection(clientSocket, protocol);
  }

void ConnectionManager::AuthorizeClientConnection(const int &clientSocket, const ConnectionProtocol &protocol){
    AuthorizeBody body;

    const unsigned char* bufferPtr = protocol.buffer.data();
      
    int usernameSize = 0, passwordSize = 0;
    
    memcpy(&usernameSize, bufferPtr, sizeof(int));
    bufferPtr += sizeof(int);

    body.username.resize(usernameSize);
    memcpy(body.username.data(), bufferPtr, usernameSize);
    bufferPtr += usernameSize;

    memcpy(&passwordSize, bufferPtr, sizeof(int));
    bufferPtr += sizeof(int);
    
    body.password.resize(passwordSize);
    memcpy(body.password.data(), bufferPtr, passwordSize);
    bufferPtr += passwordSize;

    if (body.username == "natedrake7" && body.password == "kalispera") {
      cout << "SuccessFully Authorized!" << endl;
      //send response to client that verification is successfull

      
      
    }
    else {
      //close connection with client, invalid credentials
    }
}
} // Server