#include "ConnectionManager.h"

#include "../Server.h"
#include "../../QueryPipeline/Parser/Parser.h"
#include "../../Systemic/Network/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h"
#include "../../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h"
#include "../../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h"
#include "../Threadpool/ThreadPool.h"
#include "../Server.Constants.h"

#include <atomic>
#include <cstring>
#include <iostream>
#include <ostream>
#include <stdexcept>
#include <fcntl.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "ws2_32.lib")  // Optional if using MSVC
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

    this->epollFileDescriptor = ServerConstants::INVALID_FILE_DESCRIPTOR;
    this->serverSocket = ServerConstants::INVALID_FILE_DESCRIPTOR;
  }

  ConnectionParameters::ConnectionParameters(const string& hostname, const int& port, const int& numberOfConnections, const int& timeoutTime){
    this->hostName = hostname;
    this->numberOfConnections = numberOfConnections;
    this->timeoutTime = timeoutTime;
    this->port = port;

    this->epollFileDescriptor = ServerConstants::INVALID_FILE_DESCRIPTOR;
    this->serverSocket = ServerConstants::INVALID_FILE_DESCRIPTOR;
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
    // this->events.resize(this->parameters.numberOfConnections);

    this->threadPool.InitializeWorkers(isServerRunning, 20);

    int eventCount = 0;
    while (isServerRunning) {
#ifdef _WIN32
	    const int _ = WSAPoll(this->events.data(), this->events.size(), 10);
      eventCount = this->events.size();
#else
        const int currentEvents = epoll_wait(this->parameters.epollFileDescriptor, this->events.data(), this->events.size(), 10);
        eventCount = currentEvents;
#endif

      if (eventCount < 0) {
        std::cerr << "epoll_wait failed"<< strerror(errno) << endl;
        break;
      }

      for (int i = 0;i < eventCount; i++) {

        if (!eventMutexes[i].try_lock())
            continue;

        eventMutexes[i].unlock();
        
#ifdef _WIN32
        const auto& evt = this->events[i];

        if (evt.revents & (POLLHUP | POLLERR | POLLNVAL)) {
          this->HandleClientDisconnection(evt,eventCount, i);
          continue;
        }

        if (!(evt.revents & POLLIN))
            continue;

        if (evt.fd != this->parameters.serverSocket) {
            ConnectionManager::HandleClientConnection(evt.fd, eventMutexes[i]);
            continue;
        }

        const auto clientSocket = accept(this->parameters.serverSocket, nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET) {
            cerr << "Failed to accept client (Windows)" << endl;
            continue;
        }
        
        u_long mode = 1;
        ioctlsocket(clientSocket, FIONBIO, &mode);

        pollfd newEvent{};
        newEvent.fd = clientSocket;
        newEvent.events = POLLIN;
        newEvent.revents = 0;
        this->events.push_back(std::move(newEvent));
        std::cout << "Accepted client (Windows): " << clientSocket << std::endl;

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

    if (::bind(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0)
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

    shutdown(this->parameters.serverSocket, SD_BOTH);
    closesocket(this->parameters.serverSocket);
    WSACleanup();

#else
    for (const auto&[event, data] : this->events) {
        epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, data.fd, nullptr);
        close(data.fd);
    }

    shutdown(this->parameters.serverSocket, SHUT_RDWR);
    //close(this->parameters.serverSocket);
    close(this->parameters.epollFileDescriptor);
#endif
}

  void ConnectionManager::HandleClientDisconnection(const SocketEvent& event, int& totalEvents, int& index){
    std::cout << "Client disconnected: " << event.fd << std::endl;

    this->CloseClientConnection(event.fd);

    this->events.erase(this->events.begin() + index);
    index--;
    totalEvents--;
  }

  void ConnectionManager::CloseClientConnection(const int &clientSocket) const
  {

#ifdef _WIN32
    shutdown(clientSocket, SD_BOTH);
    closesocket(clientSocket);
#else
    shutdown(clientSocket, SHUT_RDWR);
    epoll_ctl(this->parameters.epollFileDescriptor, EPOLL_CTL_DEL, clientSocket, nullptr);
    close(clientSocket);
#endif
  }

  void ConnectionManager::HandleClientConnection(const int &clientSocket, mutex& clientMutex){
    std::unique_lock<std::mutex> clientLock(clientMutex);

    Network::ConnectionProtocolHeader header;
    std::vector<char> buffer(Network::ConnectionProtocolHeader::GetSize());

    const auto headerBytesRead = recv(clientSocket, buffer.data(), Network::ConnectionProtocolHeader::GetSize(), 0);
    header.Deserialize(buffer);

    if (headerBytesRead > 0) {
      this->ReadBodyFromClient(clientSocket, header);
      return;
    }
    
    if (headerBytesRead == 0) {
      ConnectionManager::CloseClientConnection(clientSocket);
      return;
    }
    
    perror("recv failed");
  }

  void ConnectionManager::ReadBodyFromClient(const int& clientSocket, const Network::ConnectionProtocolHeader &header){
    vector<char> buffer(header.size);
    
    if (recv(clientSocket, buffer.data(), header.size, 0) <= 0) {
      std::cerr << "Failed to read body from client or body was empty" << std::endl;
      return;
    }

    switch (header.type) {
      case Network::Authorize:
        this->AuthorizeClientConnection(clientSocket, header, buffer);
        return;
      case Network::Query:
        this->GetQueryFromClient(clientSocket, header, buffer);
        return;
      case Network::Invalid:
      default:
        break;
    }
    
    //invalid request type
  }

void ConnectionManager::AuthorizeClientConnection(const int &clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char>& buffer)const {
    Network::AuthorizeProtocol protocol(header);

    protocol.Deserialize(buffer);
    auto& server = Server::ServerInstance::Get();

    const auto* user = server.Authenticate(protocol.GetUsername(), protocol.GetPassword());

    const auto* newSession = server.CreateSession(user);

    if (user != nullptr) {
      Network::ResponseProtocol responseProtocol(ResponseType::Authenticated, newSession->sessionId);
      
      ConnectionManager::SendToClient(clientSocket, &responseProtocol);
    }
    else {
      Network::ResponseProtocol responseProtocol(ResponseType::InvalidCredentials, DataTypes::Guid::Empty());
      ConnectionManager::SendToClient(clientSocket, &responseProtocol);

      this->CloseClientConnection(clientSocket);
    }
}

void ConnectionManager::GetQueryFromClient(const int &clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char> &buffer){
    Network::QueryProtocol protocol(header);

    protocol.Deserialize(buffer);

    this->threadPool.Enqueue([query = protocol.GetQuery(), clientSocket, header] {

      std::vector<QueryResult> results;
      std::vector<std::string> displayColumns;
      const auto status = QueryPipeline::Parser::Parse(query, header.sessionId, &results, &displayColumns);

      Network::QueryResponseProtocol response(status.hasError, status.message, displayColumns, results);
      
      ConnectionManager::SendToClient(clientSocket, &response);
    });
}

void ConnectionManager::SendToClient(const int &clientSocket, Network::ResponseProtocol *protocol){
    if (protocol == nullptr)
      return;

    const auto& packet = protocol->GetSerializedProtocol();

    const auto bytesSent = send(clientSocket, packet.data(), protocol->GetSize(), 0);

    if (bytesSent > 0)
      return;
    
    if (bytesSent < 0) {
      std::cerr << " Failed to send request to client"<< strerror(errno) << endl;
      return;
    }

    cout << "Client disconnected" << endl;
  }


} // Server