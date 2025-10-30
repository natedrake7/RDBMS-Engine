#include <stdexcept>
#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <cstring>
#include <sstream>

#include "../Systemic/Converter/Converter.h"
#include "../Systemic/Network/Protocols/ConnectionProtocol/AuthorizeProtocol/AuthorizeProtocol.h"
#include "../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryProtocol.h"

#include "client.h"

#include "../Systemic/Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h"

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

void shutdownServer(int signal) {
  cout << endl << "Client shutting down..." << endl;

  CloseConnection(parameters);

  exit(0);
}
 

int main()
{
  signal(SIGINT, shutdownServer);   // Ctrl+C
  signal(SIGTERM, shutdownServer);  // kill command
  signal(SIGABRT, shutdownServer);  // abort()

  const std::vector connectionString = {
    std::string("-h"),
    std::string("127.0.0.5"),
    std::string("-P"),
    std::string("1433"),
    std::string("-p"),
    std::string("kalispera"),
    std::string("-u"),
    std::string("natedrake7")
  };

  ValidateConnectionString(parameters, connectionString);
  InitializeConnectionToServer(parameters);

  cout << "Please enter the query: " << endl;

  while(true){
    std::string input;
    
    std::getline(std::cin, input);

    if(input == "exit")
      break;

    Network::QueryProtocol protocol(input);
    
    const auto& serializedProtocol = protocol.GetSerializedProtocol();

    const auto bytesSent = send(parameters.socket, serializedProtocol.data(), protocol.GetSize(), 0);

    if(bytesSent < 0)
    {
      cerr << "Failed to send request to server" << endl;
      CloseConnection(parameters);
      return -1;
    }

    if (bytesSent == 0) {
      cout << "Connection lost" << endl;
      CloseConnection(parameters);
    }

    Network::ResponseProtocolHeader responseHeader;
    std::vector<char> buffer;

    auto bytesReceived = recv(parameters.socket, reinterpret_cast<char *>(&responseHeader), responseHeader.GetSize(), 0);

    if (bytesReceived < 0) {
      cerr << "Failed to get response from server" << endl;
      CloseConnection(parameters);
      return -1;
    }

    if (bytesReceived == 0) {
      cout << "Connection lost" << endl;
      CloseConnection(parameters);
    }

    if (responseHeader.statusCode != ResponseType::QueryResponse) {
      cerr << "Failed to get response from server" << endl;
      CloseConnection(parameters);
    }

    Network::QueryResponseProtocol queryResponseProtocol(responseHeader);

    buffer.resize(responseHeader.size);

    bytesReceived = recv(parameters.socket, buffer.data(), responseHeader.size, 0);
    
    if (bytesReceived < 0) {
      cerr << "Failed to get response from server" << endl;
      CloseConnection(parameters);
      return -1;
    }

    if (bytesReceived == 0) {
      cout << "Connection lost" << endl;
      CloseConnection(parameters);
    }

    queryResponseProtocol.Deserialize(buffer);

    cout << queryResponseProtocol << endl;
  }

  CloseConnection(parameters);
  cout << "Connection Closed" << endl;

  return 0;
}

void AuthorizeClientConnection(const int& socket, const ConnectionParameters& params) {
  Network::AuthorizeProtocol protocol(params.username, params.password);

  const auto& serializedObject = protocol.GetSerializedProtocol();

  const auto bytesSent = send(socket, serializedObject.data(), serializedObject.size(), 0);

  cout << bytesSent << endl;

  if (bytesSent < 0) {
    cerr << "Failed to send request to server" << endl;
    return;
  }

  Network::ResponseProtocol responseProtocol;

  const int responseProtocolSize = responseProtocol.GetSize();

  vector<char> buffer(responseProtocolSize);

  const auto bytesReceived = recv(socket, buffer.data(), responseProtocolSize, 0);

  if (bytesReceived < 0) {
    cerr << "Failed to receive response from server" << endl;
  }

  if (bytesReceived == 0) {
    cout << "Connection to server has been lost" << endl;
  }

  responseProtocol.Deserialize(buffer);

  const auto& statusCode = responseProtocol.GetResponseType();

  if (statusCode == ResponseType::InvalidCredentials)
    throw std::runtime_error("Failed to authenticate");

  if (statusCode != ResponseType::Authenticated)
    throw std::runtime_error("Unknown error occurred");

  cout << "Successfully authenticated" << endl;
}

void InitializeConnectionToServer(ConnectionParameters& params) {
  #ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
      throw runtime_error("WSAStartup failed");
  #endif

  const auto sock = socket(AF_INET, SOCK_STREAM, 0);

  if (sock < 0)
    throw runtime_error("Failed to initialize socket");

  sockaddr_in serverAddress = {};

  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(params.port);

  inet_pton(AF_INET, params.hostName.c_str(), &serverAddress.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0) {
    #ifdef _WIN32
        closesocket(sock);
        WSACleanup();
    #else
        close(sock);
    #endif

    std::ostringstream oss;
    oss << "Failed to connect to host: " << params.hostName << ":" << params.port;

    throw runtime_error(oss.str());
  }

  AuthorizeClientConnection(sock, params);


  params.socket = sock;
}

void CloseConnection(ConnectionParameters &params)
{
  #ifdef _WIN32
    closesocket(params.socket);
    WSACleanup();
  #else
    close(params.socket);
  #endif
}

void ValidateConnectionString(ConnectionParameters& params, const vector<string>& connectionString) {
  
  for (int i = 0; i < connectionString.size(); i++) {
    const auto& parameter = connectionString[i];

    if (i + 1 >= connectionString.size())
      throw invalid_argument("invalid argument specified in connection string!");
    
    if (parameter == "-P") {
      params.port = Converter<int32_t>::Stoi(connectionString[++i]);
      continue;
    }
    if (parameter == "-h") {
      params.hostName = connectionString[++i];
      continue;
    }
    if (parameter == "-u") {
      params.username = connectionString[++i];
      continue;
    }
    if (parameter == "-p") {
      params.password = connectionString[++i];
      continue;
    }
    
    throw invalid_argument("invalid argument specified in connection string!");
  }
}