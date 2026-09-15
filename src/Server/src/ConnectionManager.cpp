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
#include "ValidationMessages.h"
#include "../../Systemic/include/Network/PayloadReader.h"

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
        this->_threadPool.InitializeWorkers(isServerRunning, 20);

        Int eventCount = 0;
        while (isServerRunning.load(std::memory_order_relaxed)){
            this->BuildEventsSet();
#ifdef _WIN32
            const auto ready = WSAPoll(
                this->_events.data(),
                static_cast<ULONG>(this->_events.size()),
                this->_parameters.timeoutTime
            );
#else
            const auto ready = epoll_wait(
                this->_parameters.epollFileDescriptor,
                this->_events.data(),
                this->_events.size(),
                this->_parameters.timeoutTime
            );
#endif
            if (ready < 0){
                if (IsInterrupted())
                    continue;

                std::cerr << "epoll_wait failed " << strerror(errno) << endl;
            }

#ifdef _WIN32
            for (const auto& event : this->_events){
                if (event.revents == 0)
                    continue;

                const auto socket = event.fd;
                const auto hangup   = (event.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0;
                const auto readable = (event.revents & POLLIN)  != 0;
                const auto writable = (event.revents & POLLOUT) != 0;
#else
            for (Int i = 0;i < ready; i++){
                const auto socket = event.data.fd;
                const auto hangup   = (event.events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) != 0;
                const auto readable = (event.events & EPOLLIN)  != 0;
                const auto writable = (event.events & EPOLLOUT) != 0;
#endif

                if (socket == this->_parameters.serverSocket){
                    if (readable)
                        this->AcceptNewConnections();

                    continue;
                }

                std::shared_ptr<ClientConnection> connection;
                if (!this->_connectionPool.TryGetValue(socket, connection))
                    continue;

                if (writable && connection->DrainWrites() == Transport::IoStatus::Failed){
                    this->CloseConnection(connection);
                    continue;
                }

                if (readable)
                    this->ServiceReadable(connection);

                if (hangup || connection->IsClosing())
                    this->CloseConnection(connection);
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

    void ConnectionManager::ServiceReadable(const std::shared_ptr<ClientConnection>& connection) const{
        const auto fillStatus = connection->FillReadBuffer();

        Header header;
        const char* payload = nullptr;

        while (true){
            const auto frameStatus = connection->TryTakeHeader(header, payload);

            if (frameStatus == HeaderStatus::Incomplete)
                break;

            if (frameStatus == HeaderStatus::Malformed){
                std::cerr << "Malformed header on socket: " << connection->Socket() << std::endl;
                connection->MarkClosing();
                return;
            }

            this->DispatchRequest(connection, header, payload);
        }
    }

    void ConnectionManager::DispatchRequest(
        const std::shared_ptr<ClientConnection>& connection,
        const Header& header,
        const char* payload
    ){
        switch (header._messageType) {
        case MessageType::Authorize:
            ConnectionManager::HandleAuthorize(connection, header, payload);
            return;
        case MessageType::Query:
            return;
        default:
            break;
        }
    }

    void ConnectionManager::HandleAuthorize(
        const std::shared_ptr<ClientConnection>& connection,
        const Header& header,
        const char* payload
    ) {
        PayloadReader reader(payload, header._payloadLength);

        DataTypes::StringView username;
        DataTypes::StringView password;

        if (!reader.ReadStringView(username) || !reader.ReadStringView(password)) {
            std::cerr << "Failed to read username or password" << std::endl;
            return;
        }

        static auto& server = Server::Get();
        const auto* user = server.Authenticate(username, password);

        if (user == nullptr) {
            std::cerr << "Failed to authenticate user" << std::endl;
            connection->SendTextFrame(MessageType::AuthFailed, header._requestId, 0, Messages::AUTH_REQUEST_INVALID_CREDENTIALS);
            connection->MarkClosing();
            return;
        }

        connection->Bind(server.CreateSession(user));
        connection->SendControlFrame(MessageType::AuthOk, header._requestId);
    }

    void ConnectionManager::HandleQuery(
        const std::shared_ptr<ClientConnection>& connection,
        const Header& header,
        const char* payload
    ){
        if (!connection->IsAuthenticated()){
            connection->SendTextFrame(MessageType::Error, header._requestId, 0, Messages::QUERY_REQUEST_NOT_AUTHENTICATED);
            connection->MarkClosing();
            return;
        }

        if (!connection->TryBeginQuery()){
            connection->SendTextFrame(MessageType::Error, header._requestId, 0, Messages::QUERY_REQUEST_QUERY_ALREADY_RUNNING);
            return;
        }

        std::string query(payload, header._payloadLength);
        this->_threadPool.Enqueue(
            [connection, header, query = std::move(query)]() mutable{
                ConnectionManager::ExecuteQuery(connection, header._requestId, std::move(query));
            });
    }

    void ConnectionManager::ExecuteQuery(
        const std::shared_ptr<ClientConnection>& connection,
        UnsignedInt requestId,
        std::string query
    ){
        auto queryContext = QueryPipeline::Parser::StartTransaction(query, connection->SessionId());

        if (queryContext.status.hasError){
            connection->SendTextFrame(
                MessageType::Error,
                requestId, 0,
                DataTypes::StringView::ViewOf(queryContext.status.message)
            );
            connection->SendControlFrame(MessageType::QueryComplete, requestId);
            queryContext.Release();
            return;
        }

        UnsignedInt statementOrdinal = 0;
        for (auto* cursor: queryContext.cursors){
            auto failed = false;

            while (cursor->CanFetch()){
                if (connection->IsClosing())
                    break;

                auto batch = cursor->FetchNextBatch();
                if (!batch.status.IsOk()){
                    connection->SendTextFrame(
                        MessageType::Error,
                        requestId, statementOrdinal,
                        DataTypes::StringView::ViewOf(batch.status.message)
                    );
                    failed = true;
                    break;
                }

                connection->WaitForWriteDrain();
            }

            if (failed){
                QueryPipeline::Parser::RollbackTransaction(connection->SessionId(), cursor);
            }
            else{
                QueryPipeline::Parser::CommitTransaction(connection->SessionId(), cursor);
                connection->SendControlFrame(MessageType::StatementComplete, requestId);
            }
            statementOrdinal++;
        }
        connection->SendControlFrame(MessageType::QueryComplete, requestId);
        queryContext.Release();
    }
}