#include "../AdditionalLibraries/Protocols/ConnectionProtocol/AuthorizeProtocol.h"


#include <stdexcept>
#include <string>
#include <iostream>
#include <ostream>
#include <vector>

#include "../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../AdditionalLibraries/Protocols/ConnectionProtocol/ConnectionProtocol.h"

#include <cstring>
#include <sstream>

#ifdef _WIN32
  #include <winsock2.h>
  #pragma comment(lib, "ws2_32.lib")
#else
  #include <sys/socket.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

using namespace std;

typedef struct ConnectionParameters {
  int port;
  string hostName;
  string username;
  string password;

  int socket;

  ConnectionParameters() {
    this->port = 0;
    this->socket = 0;
  }
  ~ConnectionParameters() = default;
}ConnectionParameters;

static void ValidateConnectionString(ConnectionParameters& parameters, const vector<string>& connectionString);
void InitializeConnectionToServer(ConnectionParameters& parameters);
void CloseConnection(ConnectionParameters& parameters);

int main()
{
  const vector connectionString = {
    string("-h"),
    string("127.0.0.5"),
    string("-P"),
    string("1433"),
    string("-p"),
    string("kalispera"),
    string("-u"),
    string("natedrake7")
  };

  ConnectionParameters parameters;
  ValidateConnectionString(parameters, connectionString);
  InitializeConnectionToServer(parameters);

  string input;
  cout << "Please enter the query: " << endl;

  while(true){
    cin >> input;

    if(input == "exit")
      break;

    const int bytesSent = send(parameters.socket, input.data(), input.size(), 0);

    if(bytesSent < 0)
    {
      cerr << "Failed to send request to server" << endl;
      CloseConnection(parameters);
      
      return -1;
    }
  }

  CloseConnection(parameters);
  cout << "Connection Closed" << endl;

  return 0;
}

void AuthorizeClientConnection(const int& socket, const ConnectionParameters& parameters) {
  ConnectionProtocol protocol;

  const AuthorizeBody body(parameters.username, parameters.password);

  protocol.header.size = body.GetBodySize();
  protocol.header.dataType = ConnectionProtocolType::Authorize;

  const int totalRequestSize = protocol.header.size + sizeof(ConnectionProtocolHeader);
  
  protocol.buffer.resize(totalRequestSize);

  unsigned char* bufferPtr = protocol.buffer.data();

  memcpy(bufferPtr, &protocol.header, sizeof(ConnectionProtocolHeader));
  bufferPtr += sizeof(ConnectionProtocolHeader);

  const int usernameSize = parameters.username.size();

  memcpy(bufferPtr, &usernameSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, parameters.username.c_str(), usernameSize);
  bufferPtr += usernameSize;
  
  const int passwordSize = parameters.password.size();

  memcpy(bufferPtr, &passwordSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, parameters.password.c_str(), passwordSize);
  bufferPtr += passwordSize;

  const auto bytesSent = send(socket, protocol.buffer.data(), totalRequestSize, 0);

  cout << protocol.buffer.data() << endl;

  cout << bytesSent << endl;

  if (bytesSent < 0) {
    cerr << "Failed to send request to server" << endl;
    return;
  }

  // const auto bytesReceived = recv(socket, void *buf, size_t n, int flags)
}

void InitializeConnectionToServer(ConnectionParameters& parameters) {
  #ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
      throw runtime_error("WSAStartup failed")
  #endif

  int sock = socket(AF_INET, SOCK_STREAM, 0);

  if (sock < 0)
    throw runtime_error("Failed to initialize socket");

  sockaddr_in serverAddress = {};

  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(parameters.port);

  inet_pton(AF_INET, parameters.hostName.c_str(), &serverAddress.sin_addr);

  const int hasConnectedToServer = connect(sock, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress));

  cout << hasConnectedToServer << endl;
  if (hasConnectedToServer < 0) {
    #ifdef _WIN32
        closesocket(sock);
        WSACleanup();
    #else
        close(sock);
    #endif

    std::ostringstream oss;
    oss << "Failed to connect to host: " << parameters.hostName << ":" << parameters.port;

    throw runtime_error(oss.str());
  }

  AuthorizeClientConnection(sock, parameters);

  cout << "Connection established" << endl;

  parameters.socket = sock;
}

void CloseConnection(ConnectionParameters &parameters)
{
  #ifdef _WIN32
    closesocket(parameters.socket);
    WSACleanup();
  #else
    close(parameters.socket);
  #endif
}

void ValidateConnectionString(ConnectionParameters& parameters, const vector<string>& connectionString) {
  
  for (int i = 0; i < connectionString.size(); i++) {
    const auto& parameter = connectionString[i];

    if (i + 1 >= connectionString.size())
      throw invalid_argument("invalid argument specified in connection string!");
    
    if (parameter == "-P") {
      parameters.port = SafeConverter<int32_t>::SafeStoi(connectionString[++i]);
      continue;
    }
    if (parameter == "-h") {
      parameters.hostName = connectionString[++i];
      continue;
    }
    if (parameter == "-u") {
      parameters.username = connectionString[++i];
      continue;
    }
    if (parameter == "-p") {
      parameters.password = connectionString[++i];
      continue;
    }
    
    throw invalid_argument("invalid argument specified in connection string!");
  }
}