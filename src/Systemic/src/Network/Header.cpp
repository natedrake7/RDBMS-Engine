#include <Systemic/Network/Header.h>
#include <Systemic/Macros.h>

#if IS_GCC
    #include <cstring>
#endif

namespace Network{
    Header::Header()
        :   _payloadLength(0), _messageType(MessageType::Invalid),
            _frameFlags(HeaderFlags::None), _requestId(0),
            _statementOrdinal(0) {}

    void Header::Encode(char* buffer) const{
        std::memcpy(buffer, this, sizeof(Header));
    }

    void Header::Decode(const char* buffer){
        std::memcpy(this, buffer, sizeof(Header));
    }

    bool Header::IsPayloadLengthValid() const{
        return this->_payloadLength <= MAX_PAYLOAD_LENGTH;
    }
}
