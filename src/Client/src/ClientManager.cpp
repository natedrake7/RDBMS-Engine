#include "../include/ClientManager.h"
#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <sstream>

#include "../../Systemic/include/Converter.h"
#include "Decoding/ResultDecoder.h"
#include "Network/PayloadReader.h"
#include "Network/PayloadWriter.h"
#include "Network/Transport.h"

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
    bool ConnectionManager::InitializeConnectionToServer(){
#ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0){
            std::cerr << "Failed to initialize Winsock" << std::endl;
            return false;
        }
#endif
        this->_networkInitialized = true;

        this->_parameters._socket = Network::ToSocket(socket(AF_INET, SOCK_STREAM, 0));

        if (this->_parameters._socket < 0) {
            std::cerr << "Failed to initialize socket" << std::endl;
            return false;
        }

        sockaddr_in serverAddress = {};

        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = htons(static_cast<UnsignedSmallInt>(this->_parameters._port));

        if (inet_pton(AF_INET, this->_parameters._hostname.c_str(), &serverAddress.sin_addr) != 1){
            std::cerr << "Invalid host address: " << this->_parameters._hostname << std::endl;
            return false;
        }

        if (connect(this->_parameters._socket, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) < 0) {
            std::cerr << "Failed to connect to host: " << this->_parameters._hostname << ":" << this->_parameters._port << std::endl;
            return false;
        }

        return true;
    }

    void ConnectionManager::CloseConnectionToServer(){
        if (this->_parameters._socket != Network::INVALID_SOCKET_DESCRIPTOR){
            Network::Close(this->_parameters._socket);
            this->_parameters._socket = Network::INVALID_SOCKET_DESCRIPTOR;
        }

#ifdef _WIN32
        if (this->_networkInitialized)
            WSACleanup();
#endif
        this->_networkInitialized = false;
    }

    bool ConnectionManager::AuthenticateConnectionToServer(){
        std::vector<char> payload;

        const Network::PayloadWriter writer(&payload);

        writer.WriteString(this->_parameters._username);
        writer.WriteString(this->_parameters._password);

        const auto requestId = this->NextRequestId();

        if (!this->SendFrame(Network::MessageType::Authorize, requestId, payload.data(), payload.size())){
            std::cerr << "Failed to send authentication request" << std::endl;
            return false;
        }

        std::vector<char> responsePayload;
        Network::Header responseHeader;

        if (!this->ReadFrame(&responseHeader, &responsePayload)){
            std::cerr << "Connection closed during authentication" << std::endl;
            return false;
        }

        if (responseHeader._requestId != requestId){
            std::cerr << "Protocol error: unexpected request id during authentication" << std::endl;
            return false;
        }

        switch (responseHeader._messageType){
        case Network::MessageType::AuthOk:
            std::cout << "Successfully authenticated" << std::endl;
            return true;
        case Network::MessageType::AuthFailed: {
            const DataTypes::StringView responseMessage(responsePayload.data(), responsePayload.size());
            std::cerr << "Authentication failed: "
                      << responseMessage << std::endl;
            return false;
        }
        default:
            std::cerr << "Protocol error: unexpected message type during authentication" << std::endl;
            return false;
        }

        return true;
    }

    bool ConnectionManager::ReadConnectionString(const std::vector<std::string> &connectionString){
        for (auto i = 0; i < connectionString.size(); i++) {
            const auto& parameter = connectionString[i];

            if (i + 1 >= connectionString.size()) {
                std::cerr << "invalid argument specified in connection string!" << std::endl;
                return false;
            }

            if (parameter == "-P") {
                this->_parameters._port = Converter::StrToInt<Int>(connectionString[++i]);
                continue;
            }
            if (parameter == "-h") {
                this->_parameters._hostname = connectionString[++i];
                continue;
            }
            if (parameter == "-u") {
                this->_parameters._username = connectionString[++i];
                continue;
            }
            if (parameter == "-p") {
                this->_parameters._password = connectionString[++i];
                continue;
            }

            std::cerr << "invalid argument specified in connection string!" << std::endl;
            return false;
        }

        return true;
    }

    bool ConnectionManager::SendFrame(
        const Network::MessageType type,
        const UnsignedInt requestId,
        const char* payload,
        const UnsignedInt payloadLength
    ) const{
        Network::Header header;
        header._messageType = type;
        header._requestId = requestId;
        header._payloadLength = payloadLength;

        std::vector<char> buffer(Network::Header::SIZE + payloadLength);
        header.Encode(buffer.data());

        if (payloadLength > 0)
            std::memcpy(buffer.data() + Network::Header::SIZE, payload, payloadLength);

        return Network::Transport::SendAll(this->_parameters._socket, buffer.data(), buffer.size()) == Network::Transport::IoStatus::Ok;
    }

    bool ConnectionManager::ReadFrame(Network::Header* header, std::vector<char>* payload) const{
        char headerBytes[Network::Header::SIZE];

        if (Network::Transport::ReceiveExact(this->_parameters._socket, headerBytes, Network::Header::SIZE) != Network::Transport::IoStatus::Ok)
            return false;

        header->Decode(headerBytes);
        payload->resize(header->_payloadLength);

        return Network::Transport::ReceiveExact(this->_parameters._socket, payload->data(), header->_payloadLength) == Network::Transport::IoStatus::Ok;
    }

    bool ConnectionManager::ReadQueryResponse(const UnsignedInt requestId) const{
        std::vector<char> buffer;
        Network::Header header;
        UnsignedInt statementsCompleted = 0;

        ResultDecoder decoder;
        while (true){
            if (!this->ReadFrame(&header, &buffer)){
                std::cerr << "Connection closed during query response" << std::endl;
                return false;
            }

            if (header._requestId != requestId){
                std::cerr << "Protocol error: received a frame for request " << header._requestId
                         << " while waiting for " << requestId << std::endl;
                return false;
            }

            switch (header._messageType) {
            case Network::MessageType::Error:
                std::cerr << "Query failed: " << DataTypes::StringView(buffer.data(), buffer.size()) << std::endl;
                break;
            case Network::MessageType::RowDescription:{
                if (!decoder.DecodeRowDescription(&buffer)){
                    std::cerr << "Protocol error: failed to decode row description" << std::endl;
                    return false;
                }

                decoder.PrintHeader(std::cout);
                break;
            }
            case Network::MessageType::DataBatch:{
                if (!decoder.DecodeDataBatch(&buffer)){
                    std::cerr << "Protocol error: failed to decode data batch" << std::endl;
                    return false;
                }

                decoder.PrintBatch(std::cout);
                break;
            }
            case Network::MessageType::StatementComplete:
                statementsCompleted++;
                break;
            case Network::MessageType::QueryComplete:
                std::cout << "Statements completed: " << statementsCompleted << std::endl;
                return true;
            default:
                std::cerr   << "Protocol error: unexpected message type "
                            << static_cast<UnsignedSmallInt>(header._messageType) << std::endl;
                return false;
            }
        }
    }

    ConnectionManager::~ConnectionManager() {
        this->CloseConnectionToServer();
    }

    bool ConnectionManager::ConnectToServer(const std::vector<std::string> &connectionString){
        if (!this->ReadConnectionString(connectionString))
            return false;

        if (!this->InitializeConnectionToServer()){
            this->CloseConnectionToServer();
            return false;
        }

        if (!this->AuthenticateConnectionToServer()){
            this->CloseConnectionToServer();
            return false;
        }

        return true;
    }

    bool ConnectionManager::ExecuteQuery(const std::string &query){
        if (query.size() > Network::Header::MAX_PAYLOAD_LENGTH){
            std::cerr << "Query exceeds maximum payload length" << std::endl;
            return false;
        }

        const auto requestId = this->NextRequestId();
        const auto start = std::chrono::high_resolution_clock::now();

        if (!this->SendFrame(Network::MessageType::Query, requestId, query.data(), query.size())){
            std::cerr << "Failed to send query" << std::endl;
            return false;
        }

        return this->ReadQueryResponse(requestId);
  }


}
