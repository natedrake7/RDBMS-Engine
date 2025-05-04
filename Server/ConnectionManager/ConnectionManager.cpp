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
  #include <netinet/in.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

namespace Server {

  void HandleNewConnections(ConnectionParameters &parameters, const atomic<bool>& isServerRunning)
  {
    while (isServerRunning) {
      
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
      serverAddress.sin_addr.s_addr = inet_addr(parameters.hostName.c_str());
      serverAddress.sin_port = htons(parameters.port);

      if (bind(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0)
        throw runtime_error("Failed to bind socket");

      parameters.socket = sock;

      if (listen(sock, SOMAXCONN) < 0) {
        CloseServerConnection(parameters);
        throw runtime_error("Failed to listen on socket");
      }

      sockaddr_in clientAddress = {};
      socklen_t clientSize = sizeof(clientAddress);
    
      const int clientSocket = accept(parameters.socket, reinterpret_cast<sockaddr*>(&clientAddress), &clientSize);

      if (clientSocket < 0) {
        std::cerr << "Connection with client failed to establish" << endl;
      }

      parameters.clientSocket = clientSocket;
    }

    cout << "Closing connections" << endl;

    CloseServerConnection(parameters);
  }

  void CloseServerConnection(const ConnectionParameters &parameters)
  {
    #ifdef _WIN32
        closesocket(parameters.socket);
        WSACleanup();
    #else
        close(parameters.socket);
    #endif
  }

  void CloseClientConnection(const ConnectionParameters &parameters)
  {
    #ifdef _WIN32
        closesocket(parameters.clientSocket);
        WSACleanup();
    #else
        close(parameters.clientSocket);
    #endif
  }

} // Server