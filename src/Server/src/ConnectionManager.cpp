#include "../include/ConnectionManager.h"

#include "../include/Server.h"
#include "../../QueryPipeline/include/Parser.h"
#include "../../Systemic/include/Network/AuthorizeProtocol.h"
#include "../../Systemic/include/Network/QueryProtocol.h"
#include "../../Systemic/include/Network/QueryResponseProtocol.h"
#include "../include/ThreadPool.h"
#include "../include/Constants.h"

#include <atomic>
#include <iostream>
#include <ostream>
#include <stdexcept>
#include <utility>
#include <fcntl.h>
#include <ranges>

#include "ClientConnection.h"

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

namespace Network {
    ConnectionParameters::ConnectionParameters()
        :   port(1433), serverSocket(Constants::INVALID_FILE_DESCRIPTOR),
            epollFileDescriptor(Constants::INVALID_FILE_DESCRIPTOR), hostName("127.0.0.1"),
            numberOfConnections(20), timeoutTime(10){}

    ConnectionParameters::ConnectionParameters(
        string& hostname, const Int port,
        const Int numberOfConnections, const Int timeoutTime
    ) : port(port), serverSocket(Constants::INVALID_FILE_DESCRIPTOR),
        epollFileDescriptor(Constants::INVALID_FILE_DESCRIPTOR), hostName(std::move(hostname)),
        numberOfConnections(numberOfConnections), timeoutTime(timeoutTime){}

    void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning){
        ConnectionManager _connectionManager(parameters);
        _connectionManager.HandleNewConnections(isServerRunning);
    }

    ConnectionManager::ConnectionManager(const ConnectionParameters &parameters){
        this->_parameters = parameters;
    }

    void ConnectionManager::HandleNewConnections(const atomic<bool>& isServerRunning){
        this->InitializeServerSocket();

        std::vector<mutex> eventMutexes(this->_parameters.numberOfConnections);
        this->_events.resize(this->_parameters.numberOfConnections);

        this->_threadPool.InitializeWorkers(isServerRunning, 20);

        Int eventCount = 0;
        while (isServerRunning) {
#ifdef _WIN32
	        const auto _ = WSAPoll(this->_events.data(), this->_events.size(), 10);
            eventCount = this->_events.size();
#else
            const auto currentEvents = epoll_wait(this->_parameters.epollFileDescriptor, this->_events.data(), this->_events.size(), 10);
            eventCount = currentEvents;
#endif

          if (eventCount < 0 && errno != EINTR) {
            std::cerr << "epoll_wait failed " << strerror(errno) << endl;
            break;
          }

          for (int i = 0;i < eventCount; i++) {

            if (!eventMutexes[i].try_lock())
                continue;

            eventMutexes[i].unlock();
        
#ifdef _WIN32
            const auto& evt = this->_events[i];

            if (evt.revents & (POLLHUP | POLLERR | POLLNVAL)) {
              this->HandleClientDisconnection(evt,eventCount, i);
              continue;
            }

            if (!(evt.revents & POLLIN))
                continue;

            if (evt.fd != this->_parameters.serverSocket) {
                ConnectionManager::HandleClientConnection(evt.fd, eventMutexes[i]);
                continue;
            }

            const auto clientSocket = accept(this->_parameters.serverSocket, nullptr, nullptr);
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
            this->_events.push_back(std::move(newEvent));
            std::cout << "Accepted client (Windows): " << clientSocket << std::endl;

#else
            auto& evt = this->_events[i];

            if (evt.events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
              this->HandleClientDisconnection(evt.data.fd, eventCount, i);
              continue;
            }

            if (!(evt.events & EPOLLIN))
                continue;

            if (evt.data.fd != this->_parameters.serverSocket) {
                ConnectionManager::HandleClientConnection(evt.data.fd, eventMutexes[i]);
                continue;
            }

            sockaddr_in clientAddress{};
            socklen_t clientSize = sizeof(clientAddress);
            const int clientSocket = accept(this->_parameters.serverSocket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);

            if (clientSocket < 0) {
                cerr << "Failed to accept client (Linux)" << endl;
                continue;
            }

            fcntl(clientSocket, F_SETFL, fcntl(clientSocket, F_GETFL, 0) | O_NONBLOCK);

            epoll_event newEvent{};
            newEvent.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
            newEvent.data.fd = clientSocket;
            epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_ADD, clientSocket, &newEvent);
            cout << "Accepted client (Linux): " << clientSocket << endl;
#endif
      }
    }

        std::cout << "Closing connections" << endl;
        this->CloseServerConnection();
  }

  void ConnectionManager::InitializeServerSocket(){
            
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
      throw std::runtime_error( "WSAStartup failed");
#endif

    const auto sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0)
      throw std::runtime_error("Failed to create socket");

#ifdef _WIN32
    u_long mode = 1;
    ioctlsocket(sock, FIONBIO, &mode);
#else
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#endif

    sockaddr_in serverAddress = {};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = inet_addr(this->_parameters.hostName.c_str());
    serverAddress.sin_port = htons(this->_parameters.port);

    if (::bind(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0)
      throw std::runtime_error("Failed to bind socket");
    
    if (listen(sock, SOMAXCONN) < 0) {
      this->CloseServerConnection();
      throw std::runtime_error("Failed to listen on socket");
    }

#ifdef _WIN32
    pollfd pfd{};
    pfd.fd = sock;
    pfd.events = POLLIN;
    pfd.revents = 0;

    this->_events.push_back(pfd);
#else
    const int epollFd = epoll_create1(0);
    if (epollFd == -1)
      throw std::runtime_error("Failed to create epoll file descriptor");

    this->_parameters.epollFileDescriptor = epollFd;

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = sock;

    if (epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_ADD, sock, &event) == -1) {
      perror("epoll_ctl failed to add server socket");
    }
#endif

    this->_parameters.serverSocket = sock;

  }

void ConnectionManager::CloseServerConnection() const
{
#ifdef _WIN32
    for (const auto& event : this->_events)
        closesocket(event.fd);

    shutdown(this->_parameters.serverSocket, SD_BOTH);
    closesocket(this->_parameters.serverSocket);
    WSACleanup();

#else
    for (const auto&[event, data] : this->_events) {
        epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_DEL, data.fd, nullptr);
        close(data.fd);
    }

    shutdown(this->_parameters.serverSocket, SHUT_RDWR);
    //close(this->parameters.serverSocket);
    close(this->_parameters.epollFileDescriptor);
#endif
}

    void ConnectionManager::AcceptNewConnections(){
        while (true){
            const auto clientSocket = accept(this->_parameters.serverSocket, nullptr, nullptr);

            if (clientSocket < 0){
                if (!WouldBlock())
                    std::cerr << "Failed to accept client" << std::endl;

                return;
            }

            SetNonBlocking(clientSocket);
            auto connection = std::make_shared<ClientConnection>(clientSocket);
            this->_connectionPool.Add(clientSocket, connection);
#ifndef _WIN32
            epoll_event event{};
            event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
            event.data.fd = clientSocket;
            epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_ADD, clientSocket, &event);
#endif
            std::cout << "Accepted client: " << clientSocket << std::endl;
        }
    }

    void ConnectionManager::CloseConnection(const std::shared_ptr<ClientConnection>& connection){
        const auto socket = connection->Socket();

        // Wakes any worker parked in WaitForWriteDrain on this connection.
        connection->MarkClosing();

#ifdef _WIN32
        shutdown(socket, SD_BOTH);
#else
        shutdown(socket, SHUT_RDWR);
        epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_DEL, socket, nullptr);
#endif
        // Drop the reactor's reference. The fd is NOT closed here: a worker may
        // still hold a shared_ptr, and closing would free the number for reuse by
        // the next accept - which would stream one user's rows onto another's socket.
        // ~ClientConnection closes it when the last reference goes.
        this->_connectionPool.Remove(socket);

        std::cout << "Client disconnected: " << socket << std::endl;
    }

    void ConnectionManager::CloseServerConnection(){
        // Connection destructors close their own sockets.
        this->_connectionPool.clear();
        this->_events.clear();

        shutdown(this->_parameters.serverSocket, SHUT_RDWR_COMPAT);
        Close(this->_parameters.serverSocket);

#ifdef _WIN32
        WSACleanup();
#else
        Close(this->_parameters.epollFileDescriptor);
#endif
    }

    void ConnectionManager::BuildEventsSet(){
#ifdef _WIN32
        this->_events.clear();
        this->_events.reserve(this->_connectionPool.size() + 1);

        pollfd listener{};
        listener.fd = this->_parameters.serverSocket;
        listener.events = POLLIN;
        this->_events.push_back(listener);

        for (const auto& connection : this->_connectionPool | std::views::values) {
            pollfd entry{};
            entry.fd = connection->Socket();
            entry.events = static_cast<short>(POLLIN | (connection->WantsWrite() ? POLLOUT : 0));
            this->_events.push_back(entry);
        }
#else
        // epoll holds the set kernel-side; only re-arm interest that changed.
        for (const auto& connection : this->_connectionPool | std::views::values)
            this->UpdateWriteInterest(connection);

        this->_events.resize(this->_connectionPool.size() + 1);
#endif
    }

    void ConnectionManager::UpdateWriteInterest(const std::shared_ptr<ClientConnection>& connection) const{
#ifndef _WIN32
        epoll_event event{};
        event.data.fd = connection->Socket();
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

        if (connection->WantsWrite())
            event.events |= EPOLLOUT;

        epoll_ctl(this->_parameters.epollFileDescriptor, EPOLL_CTL_MOD, connection->Socket(), &event);
#endif
    }

    bool ConnectionManager::IsServiceReadable(std::shared_ptr<ClientConnection>& connection) const{
        const auto fillStatus = connection->Re();
    }


#ifdef WIN32
  void ConnectionManager::HandleClientDisconnection(const SocketEvent& event, int& totalEvents, int& index){
    std::cout << "Client disconnected: " << event.fd << std::endl;

    this->CloseClientConnection(event.fd);

    this->_events.erase(this->_events.begin() + index);
    index--;
    totalEvents--;
  }
#else
  void ConnectionManager::HandleClientDisconnection(const Int socket, int &totalEvents, int &index){
    std::cout << "Client disconnected: " << socket << std::endl;

    this->CloseClientConnection(socket);

    // this->events.erase(this->events.begin() + index);

    // index--;
    // totalEvents--;
  }
#endif


  void ConnectionManager::HandleClientConnection(const Int clientSocket, mutex& clientMutex){
    std::unique_lock<std::mutex> clientLock(clientMutex);

    std::vector<char> buffer(Network::ConnectionProtocolHeader::GetSize());

    const auto headerBytesRead = recv(clientSocket, buffer.data(), Network::ConnectionProtocolHeader::GetSize(), 0);

#ifdef WIN32
    const int err = WSAGetLastError();
    if (err == WSAEWOULDBLOCK) {
      // No data available now — just return and continue
      return;
    }
#endif

    if (headerBytesRead > 0) {
      Network::ConnectionProtocolHeader header;
      header.Deserialize(buffer);

      this->ReadBodyFromClient(clientSocket, header);
      return;
    }
    
    if (headerBytesRead == 0) {
      ConnectionManager::CloseClientConnection(clientSocket);
      return;
    }
    
    perror("recv failed");
  }

  void ConnectionManager::ReadBodyFromClient(const Int clientSocket, const Network::ConnectionProtocolHeader &header){
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

void ConnectionManager::AuthorizeClientConnection(
    const Int clientSocket,
    const Network::ConnectionProtocolHeader &header,
    const vector<char>& buffer
)const {
    Network::AuthorizeProtocol protocol(header);

    protocol.Deserialize(buffer);
    auto& server = Network::Server::Get();

    const auto* user = server.Authenticate(DataTypes::StringView(protocol.GetUsername()), DataTypes::StringView(protocol.GetPassword()));

    if (user == nullptr) {
      Network::ResponseProtocol responseProtocol(ResponseType::InvalidCredentials, DataTypes::Guid::Empty());
      ConnectionManager::SendToClient(clientSocket, &responseProtocol);

      this->CloseClientConnection(clientSocket);
      return;
    }

    const auto* newSession = server.CreateSession(user);

    Network::ResponseProtocol responseProtocol(ResponseType::Authenticated, newSession->sessionId);

    ConnectionManager::SendToClient(clientSocket, &responseProtocol);
}

void ConnectionManager::GetQueryFromClient(const Int clientSocket, const Network::ConnectionProtocolHeader &header, const vector<char> &buffer){
    Network::QueryProtocol protocol(header);

    protocol.Deserialize(buffer);

    this->_threadPool.Enqueue([query = protocol.GetQuery(), clientSocket, header] {  ConnectionManager::ExecuteQuery(query, clientSocket, header); });
}

void ConnectionManager::ExecuteQuery(const std::string& query, const Int socket, const Network::ConnectionProtocolHeader &header){
    auto compileResult = QueryPipeline::Parser::StartTransaction(query, header.sessionId);

    if (compileResult.status.hasError) {
        DataStructures::PolymorphicArray<QueryResult> results;
        Network::QueryResponseProtocol response(
            compileResult.status.hasError,
            false,
            DataTypes::StringView::ViewOf(compileResult.status.message),
            {},
            results
        );
        ConnectionManager::SendToClient(socket, &response);
        return;
    }

    bool hasError = false;
    for (auto* cursor : compileResult.cursors) {
      while (cursor->CanFetch()) {
        auto batchResult = cursor->FetchNextBatch();

        bool hasMore = batchResult.status.IsOk() && cursor->CanFetch();

        // Network::QueryResponseProtocol response(
        // batchResult.status.IsOk(),
        //         hasMore,
        //         batchResult.status.message.ToView(),
        //         batchResult.displayColumnNames,
        //      batchResult.results
        //     );
        // ConnectionManager::SendToClient(socket, &response);

        if (!batchResult.status.IsOk()) {
          hasError = true;
          break;
        }
      }

      if (hasError) {
        QueryPipeline::Parser::RollbackTransaction(header.sessionId, cursor);
        continue;
      }

      QueryPipeline::Parser::CommitTransaction(header.sessionId, cursor);
    }
}

void ConnectionManager::SendToClient(const Int clientSocket, Network::ResponseProtocol *protocol){
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