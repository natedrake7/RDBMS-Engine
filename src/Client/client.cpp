#include <stdexcept>
#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <cstring>
#include <sstream>
#include <csignal>

#include "client.h"


#include "../Systemic/Converter/Converter.h"
#include "../Systemic/Network/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h"
#include "../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h"
#include "../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h"
#include "ClientManager/ClientManager.h"


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

ConnectionParameters parameters;

DataTypes::Guid sessionId;



void ShutdownClient(int signal) {
  std::cout << "Client shutting down..." << std::endl;
  exit(0);
}

void RegisterSignals() {
  std::signal(SIGINT, ShutdownClient);   // Ctrl+C
  std::signal(SIGTERM, ShutdownClient);  // kill command
  std::signal(SIGABRT, ShutdownClient);  // abort()
}
 

int main()
{
  RegisterSignals();

  const std::vector connectionString = {
    std::string("-h"),
    std::string("127.0.0.5"),
    std::string("-P"),
    std::string("1433"),
    std::string("-p"),
    std::string("admin"),
    std::string("-u"),
    std::string("admin")
  };

  Client::ConnectionManager manager;

  if (manager.ConnectToServer(connectionString) == false)
    return -1;

  std::cout << "Please enter the query: " << std::endl;

  while(true){
    std::string query;
    
    std::getline(std::cin, query);

    if(query == "exit")
      break;

    if (manager.SendQuery(query) == false)
      break;

    if (manager.ParseQueryResponse() == false)
      break;
  }

  //notify server connection closes
  std::cout << "Connection Closed" << std::endl;
  return 0;
}