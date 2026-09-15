#pragma once
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
    struct Header;
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

    void InitializeConnectionManagerThread(const ConnectionParameters& parameters, const std::atomic<bool>& isServerRunning);

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

        void ServiceReadable(const std::shared_ptr<ClientConnection>& connection);
        void DispatchRequest(
            const std::shared_ptr<ClientConnection>& connection,
            const Header& header,
            const char* payload
        );

        static void HandleClientAuthentication(
            const std::shared_ptr<ClientConnection>& connection,
            const Header& header,
            const char* payload
        );

        void HandleQuery(
            const std::shared_ptr<ClientConnection>& connection,
            const Header& header,
            const char* payload
        );

        static void ExecuteQuery(
            const std::shared_ptr<ClientConnection>& connection,
            UnsignedInt requestId,
            std::string query
        );

    public:
        explicit ConnectionManager(const ConnectionParameters& parameters);
        ~ConnectionManager() = default;

        void HandleNewConnections(const std::atomic<bool>& isServerRunning);
  };



}