#include "../include/ClientManager.h"
#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <cstring>
#include <sstream>

#include "../../Systemic/include/Converter.h"
#include "../../Systemic/include/Network/AuthorizeProtocol.h"
#include "../../Systemic/include/Network/QueryProtocol.h"
#include "../../Systemic/include/Network/QueryResponseProtocol.h"

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


namespace Client {
  ConnectionManager::ConnectionManager() = default;

  ConnectionManager::~ConnectionManager() {
    this->CloseConnectionToServer();
  }

  bool ConnectionManager::ConnectToServer(const std::vector<std::string> &connectionString){
    if (this->ReadConnectionString(connectionString) == false)
      return false;

    if (this->InitializeConnectionToServer() == false)
      return false;

    if (this->AuthenticateConnectionToServer() == false)
      return false;

    return true;
  }

  bool ConnectionManager::SendQuery(const std::string &query)const{
    Network::QueryProtocol protocol(query, this->sessionId);

    const auto& serializedProtocol = protocol.GetSerializedProtocol();

    const auto bytesSent = send(this->parameters.socket, serializedProtocol.data(), protocol.GetSize(), 0);

    if(bytesSent < 0)
    {
      std::cerr << "Failed to send request to server" << std::endl;
      return false;
    }

    if (bytesSent == 0) {
      std::cout << "Connection lost" << std::endl;
      return false;
    }

    return true;
  }

  bool ConnectionManager::ParseQueryResponse()const{
      Network::ResponseProtocolHeader responseHeader;
      std::vector<char> buffer(Network::ResponseProtocolHeader::GetSize());

      auto bytesReceived = recv(this->parameters.socket, buffer.data(), Network::ResponseProtocolHeader::GetSize(), 0);

      responseHeader.Deserialize(buffer);

      if (bytesReceived < 0) {
        std::cerr << "Failed to get response from server" << std::endl;
        return false;
      }

      if (bytesReceived == 0) {
        std::cout << "Connection lost" << std::endl;
        return false;
      }

      if (responseHeader.statusCode != ResponseType::QueryResponse) {
        std::cerr << "Failed to get response from server" << std::endl;
        return false;
      }

      Network::QueryResponseProtocol queryResponseProtocol(responseHeader);

      buffer.resize(responseHeader.size);
      bytesReceived = recv(this->parameters.socket, buffer.data(), responseHeader.size, 0);

      if (bytesReceived < 0) {
        std::cerr << "Failed to get response from server" << std::endl;
        return false;
      }

      if (bytesReceived == 0) {
        std::cout << "Connection lost" << std::endl;
        return false;
      }

      queryResponseProtocol.Deserialize(buffer);

      std::cout << queryResponseProtocol << std::endl;
      return true;
  }

  bool ConnectionManager::ReadConnectionString(const std::vector<std::string> &connectionString){
    for (int i = 0; i < connectionString.size(); i++) {
      const auto& parameter = connectionString[i];

      if (i + 1 >= connectionString.size()) {
        std::cerr << "invalid argument specified in connection string!" << std::endl;
        return false;
      }

      if (parameter == "-P") {
        this->parameters.port = Converter<int32_t>::Stoi(connectionString[++i]);
        continue;
      }
      if (parameter == "-h") {
        this->parameters.hostName = connectionString[++i];
        continue;
      }
      if (parameter == "-u") {
        this->parameters.username = connectionString[++i];
        continue;
      }
      if (parameter == "-p") {
        this->parameters.password = connectionString[++i];
        continue;
      }

      std::cerr << "invalid argument specified in connection string!" << std::endl;
      return false;
    }

    return true;
  }

  bool ConnectionManager::InitializeConnectionToServer(){
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
      throw runtime_error("WSAStartup failed");
#endif

    this->parameters.socket = socket(AF_INET, SOCK_STREAM, 0);

    if (this->parameters.socket < 0) {
      std::cerr << "Failed to initialize socket" << std::endl;
      return false;
    }

    sockaddr_in serverAddress = {};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(this->parameters.port);

    inet_pton(AF_INET, this->parameters.hostName.c_str(), &serverAddress.sin_addr);

    if (connect(this->parameters.socket, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0) {
#ifdef _WIN32
      closesocket(this->parameters.socket);
      WSACleanup();
#else
      close(this->parameters.socket);
#endif

      std::cerr << "Failed to connect to host: " << this->parameters.hostName << ":" << this->parameters.port << std::endl;
      return false;
    }

    return true;
  }

  bool ConnectionManager::AuthenticateConnectionToServer(){
    Network::AuthorizeProtocol protocol(this->parameters.username, this->parameters.password);

    const auto& serializedObject = protocol.GetSerializedProtocol();

    const auto bytesSent = send(this->parameters.socket, serializedObject.data(), serializedObject.size(), 0);

    if (bytesSent < 0) {
      std::cerr << "Failed to send request to server" << std::endl;
      return false;
    }

    Network::ResponseProtocol responseProtocol;

    const int responseProtocolSize = responseProtocol.GetSize();

    vector<char> buffer(responseProtocolSize);

    const auto bytesReceived = recv(this->parameters.socket, buffer.data(), responseProtocolSize, 0);

    if (bytesReceived < 0) {
      std::cerr << "Failed to receive response from server" << std::endl;
      return false;
    }

    if (bytesReceived == 0) {
      std::cout << "Connection to server has been lost" << std::endl;
      return false;
    }

    responseProtocol.Deserialize(buffer);

    const auto& statusCode = responseProtocol.GetResponseType();

    if (statusCode == ResponseType::InvalidCredentials) {
      std::cerr << "Failed to authenticate user" << std::endl;
      return false;
    }

    if (statusCode != ResponseType::Authenticated) {
      std::cerr << "Unexpected response from server" << std::endl;
      return false;
    }

    this->sessionId = responseProtocol.GetSessionId();

    std::cout << "Successfully authenticated" << std::endl;
    return true;
  }

  void ConnectionManager::CloseConnectionToServer(){
#ifdef _WIN32
      closesocket(this->parameters.socket);
      WSACleanup();
#else
      close(this->parameters.socket);
#endif
  }


}
