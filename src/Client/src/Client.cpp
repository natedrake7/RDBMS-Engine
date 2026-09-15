#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <csignal>

#include "../include/Client.h"
#include "../../Systemic/include/Network/QueryResponseProtocol.h"
#include "../include/ClientManager.h"

#ifdef _WIN32
#define NOMINMAX
#define byte win_byte_override // Add this before any Windows headers
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "ws2_32.lib")
#undef byte
#else
  #include <sys/socket.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

void ShutdownClient(int signal) {
  std::cout << "Client shutting down..." << std::endl;
  exit(0);
}

void RegisterSignals() {
  std::signal(SIGINT, ShutdownClient);   // Ctrl+C
  std::signal(SIGTERM, ShutdownClient);  // kill command
  std::signal(SIGABRT, ShutdownClient);  // abort()
}
 
int main(){
    RegisterSignals();

    const std::vector connectionString = {
        std::string("-h"),
        std::string("127.0.0.1"),
        std::string("-P"),
        std::string("1433"),
        std::string("-p"),
        std::string("admin"),
        std::string("-u"),
        std::string("admin")
    };

    Client::ConnectionManager manager;

    if (!manager.ConnectToServer(connectionString))
        return -1;

    std::cout << "Please enter the query: " << std::endl;

    static constexpr DataTypes::StringView EXIT = "exit";
    std::string query;
    while(true){

        std::getline(std::cin, query);
        if(DataTypes::StringView::EqualsIgnoreCase(DataTypes::StringView::ViewOf(query), EXIT))
            break;

        if (query.empty())
            continue;

        if (!manager.ExecuteQuery(query))
            break;
    }

    //notify server connection closes
    std::cout << "Connection Closed" << std::endl;
  return 0;
}