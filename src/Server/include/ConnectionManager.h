#pragma once
#include "../../Systemic/include/Network/ResponseProtocol.h"
#include "../../Systemic/include/Network/ConnectionProtocol.h"
#include "ThreadPool.h"


#include <atomic>
#include <string>

#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/Network/Socket.h"

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


namespace Network {
    class ClientConnection;

    struct ConnectionParameters {
        Int port;
        Int serverSocket;
        Int epollFileDescriptor;
        std::string hostName;

        Int numberOfConnections;
        Int timeoutTime;

        ConnectionParameters();
        ConnectionParameters(std::string& hostname, Int port, Int numberOfConnections, Int timeoutTime);
    };

    void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const atomic<bool>& isServerRunning);

    class ConnectionManager final{
        ConnectionParameters _parameters;
        Dictionary<socket_t, std::shared_ptr<ClientConnection>> _connectionPool;
        std::vector<SocketEvent> _events;
        ThreadPool _threadPool;

        void InitializeServerSocket();
        void CloseServerConnection() const;
        void AcceptNewConnections();
        void CloseConnection(const std::shared_ptr<ClientConnection>& connection);
        void BuildEventsSet();
        void UpdateWriteInterest(const std::shared_ptr<ClientConnection>& connection)const;

        [[nodiscard]] bool IsServiceReadable(std::shared_ptr<ClientConnection>& connection)const;

        static void SendToClient(Int clientSocket, Network::ResponseProtocol* protocol);

        void GetQueryFromClient(Int clientSocket, const Network::ConnectionProtocolHeader& header, const std::vector<char>& buffer);
        void AuthorizeClientConnection(Int clientSocket, const Network::ConnectionProtocolHeader &header, const std::vector<char>& buffer)const;
        void HandleClientConnection(Int clientSocket, mutex& clientMutex);
        void ReadBodyFromClient(Int clientSocket, const Network::ConnectionProtocolHeader& header);

        static void ExecuteQuery(const std::string& query, Int socket, const Network::ConnectionProtocolHeader &header);

#ifdef _WIN32
        void HandleClientDisconnection(const SocketEvent& event, Int& totalEvents, Int& index);
#else
        void HandleClientDisconnection(Int socket, Int& totalEvents, Int& index);
#endif
    public:
        explicit ConnectionManager(const ConnectionParameters& parameters);
        ~ConnectionManager() = default;

        void HandleNewConnections(const atomic<bool>& isServerRunning);
  };



}