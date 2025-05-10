#include "ConnectionManager.h"

#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h"
#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeResponseProtocol.h"
#include "../../AdditionalLibraries/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h"

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
    vector<mutex> eventMutexes(this->parameters.numberOfConnections);
    this->events.resize(this->parameters.numberOfConnections);

    while (isServerRunning) {
#ifdef _WIN32
	    const int eventCount = WSAPoll(this->events.data(), this->events.size(), 1000);
#else
        const int eventCount = epoll_wait(this->parameters.epollFileDescriptor, this->events.data(), this->events.size(), 10);
#endif

      if (eventCount < 0) {
        std::cerr << "epoll_wait failed"<< strerror(errno) << endl;
        // break;
      }

      for (int i = 0;i < eventCount; i++) {

        if (!eventMutexes[i].try_lock())
            continue;

        eventMutexes[i].unlock();
        
#ifdef _WIN32
        auto& evt = this->events[i];
        if (!(evt.revents & POLLIN))
            continue;

        if (evt.fd != this->parameters.serverSocket) {
            ConnectionManager::HandleClientConnection(evt.fd, eventMutexes[i]);
            continue;
        }

        SOCKET clientSocket = accept(this->parameters.serverSocket, nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET) {
            cerr << "Failed to accept client (Windows)" << endl;
            continue;
        }

        pollfd newEvent{};
        newEvent.fd = clientSocket;
        newEvent.events = POLLIN;
        this->events.push_back(newEvent);
        cout << "Accepted client (Windows): " << clientSocket << endl;

#else
        auto& evt = this->events[i];
        if (!(evt.events & EPOLLIN))
            continue;

        if (evt.data.fd != this->parameters.serverSocket) {
            ConnectionManager::HandleClientConnection(evt.data.fd, eventMutexes[i]);
            continue;
        }

        sockaddr_in clientAddress{};
        socklen_t clientSize = sizeof(clientAddress);
        int clientSocket = accept(this->parameters.serverSocket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);
        if (clientSocket < 0) {
            cerr << "Failed to accept client (Linux)" << endl;
            continue;
        }

        fcntl(clientSocket, F_SETFL, fcntl(clientSocket, F_GETFL, 0) | O_NONBLOCK);

        epoll_event newEvent{};
        newEvent.events = EPOLLIN | EPOLLET;
        newEvent.data.fd = clientSocket;
        epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_ADD, clientSocket, &newEvent);
        cout << "Accepted client (Linux): " << clientSocket << endl;
#endif
      }
    }

    cout << "Closing connections" << endl;

    this->CloseServerConnection();
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
      this->CloseServerConnection();
      throw runtime_error("Failed to listen on socket");
    }

#ifdef _WIN32
    pollfd pfd{};
    pfd.fd = sock;
    pfd.events = POLLIN;
    pfd.revents = 0;

    this->events.push_back(pfd);
#else
    const int epollFd = epoll_create1(0);
    if (epollFd == -1)
      throw std::runtime_error("Failed to create epoll file descriptor");

    this->parameters.epollFileDescriptor = epollFd;

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = sock;

    epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_ADD, sock, &event);
#endif

    this->parameters.serverSocket = sock;

  }

void ConnectionManager::CloseServerConnection() const
{
#ifdef _WIN32
    for (const auto& event : this->events) 
        closesocket(event.fd);

    //closesocket(this->parameters.serverSocket);
    WSACleanup();

#else
    for (const auto& event : this->events) {
        epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, event.data.fd, nullptr);
        close(event.data.fd);
    }

    //close(this->parameters.serverSocket);
    close(this->parameters.epollFileDescriptor);
#endif
}

  void ConnectionManager::CloseClientConnection(const int &clientSocket) const
  {

#ifdef _WIN32
    closesocket(clientSocket);
    WSACleanup();
#else
    epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, clientSocket, nullptr);
    close(clientSocket);
#endif
  }

  void ConnectionManager::HandleClientConnection(const int &clientSocket, mutex& clientMutex) const{

    clientMutex.lock();
    
    ConnectionProtocolHeader header;
    const ssize_t headerBytesRead = recv(clientSocket, &header, sizeof(ConnectionProtocolHeader), 0);

    if (headerBytesRead > 0) {
      this->ReadBodyFromClient(clientSocket, header);
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

  void ConnectionManager::ReadBodyFromClient(const int& clientSocket, const ConnectionProtocolHeader &header) const{
    vector<unsigned char> buffer(header.size);
    
    if (recv(clientSocket, buffer.data(), header.size, 0) <= 0) {
      perror("failed to read body from client or body was empty!");
      return;
    }
    
    if (header.dataType == ConnectionProtocolType::Authorize) {
      this->AuthorizeClientConnection(clientSocket, header, buffer);
      return;
    }
    
    if (header.dataType == ConnectionProtocolType::Query) {
      ConnectionManager::GetQueryFromClient(clientSocket, header, buffer);
      return;
    }

    //invalid request type
  }

void ConnectionManager::AuthorizeClientConnection(const int &clientSocket, const ConnectionProtocolHeader &header, const vector<unsigned char>& buffer) const{
    AuthorizeProtocol protocol(header);

    protocol.Deserialize(buffer);

    if (protocol.GetUsername() == "natedrake7" && protocol.GetPassword() == "kalispera") {
      AuthorizeResponseProtocol responseProtocol(ResponseType::Authenticated);
      
      ConnectionManager::SendToClient(clientSocket, &responseProtocol);
    }
    else {
      AuthorizeResponseProtocol responseProtocol(ResponseType::InvalidCredentials);
      ConnectionManager::SendToClient(clientSocket, &responseProtocol);

      this->CloseClientConnection(clientSocket);
    }
}

void ConnectionManager::GetQueryFromClient(const int &clientSocket, const ConnectionProtocolHeader &header, const vector<unsigned char> &buffer){
    QueryProtocol protocol(header);

    protocol.Deserialize(buffer);

    cout << protocol.GetQuery() << endl;

    //send query to threadpool to be executed
    //
    //
    ///////////////////////////////////////////
    ///
    ///send response of the rows back to the client
}

void ConnectionManager::SendToClient(const int &clientSocket, ResponseProtocol *protocol){
    if (protocol == nullptr)
      return;

    const auto& serializedProtocol = protocol->GetSerializedProtocol();

    const auto protocolSize = protocol->GetSize();

    const auto bytesSent = send(clientSocket, serializedProtocol.data(), protocolSize, 0);

    if (bytesSent > 0)
      return;
    
    if (bytesSent < 0) {
      std::cerr << " Failed to send request to client"<< strerror(errno) << endl;
      return;
    }

    cout << "Client disconnected" << endl;
  }


} // Server