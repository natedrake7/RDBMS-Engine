#pragma once
#include "../DataTypes/DataTypes.h"

#ifdef _WIN32
    #define NOMINMAX
    #define byte win_byte_override
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #undef byte
    #define SHUT_RDWR_COMPAT SD_BOTH
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <cerrno>
    #define SHUT_RDWR_COMPAT SHUT_RDWR
#endif

namespace Network{
    typedef Int socket_t;

    [[nodiscard]] inline bool WouldBlock(){
#ifdef _WIN32
        return WSAGetLastError() == WSAEWOULDBLOCK;
#else
        return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
    }

    [[nodiscard]] inline bool IsInterrupted(){
#ifdef _WIN32
        return false;
#else
        return errno == EINTR;
#endif
    }

    inline void SetNonBlocking(const socket_t socket){
#ifdef _WIN32
        u_long nonBlocking = 1;
        ioctlsocket(socket, FIONBIO, &nonBlocking);
#else
        fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
#endif
    }

    inline void Close(const socket_t socket){
#ifdef _WIN32
        closesocket(socket);
#else
        close(socket);
#endif
    }
}