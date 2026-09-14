#pragma once
#include "../DataTypes/DataTypes.h"


namespace Network{
    enum class MessageType : UnsignedSmallInt {
        Invalid           = 0,
        // client -> server
        Authorize         = 1,
        Query             = 2,
        CancelRequest     = 3,
        // server -> client
        AuthOk            = 128,
        AuthFailed        = 129,
        Error             = 130,
        RowDescription    = 131,
        DataBatch         = 132,
        StatementComplete = 133,
        QueryComplete     = 134
    };

    enum class HeaderFlags : UnsignedSmallInt {
        None = 0,
        HasMore = 1 << 0
    };

    enum class HeaderStatus : UnsignedTinyInt{
        Incomplete = 0,   // wait for more bytes
        Ready      = 1,
        Malformed  = 2    // protocol violation - caller must close the connection
    };

    struct Header{
        UnsignedInt _payloadLength;
        MessageType _messageType;
        HeaderFlags _frameFlags;
        UnsignedInt _requestId;
        UnsignedInt _statementOrdinal;

        static constexpr auto SIZE =
                sizeof(UnsignedInt) + sizeof(MessageType)
                + sizeof(HeaderFlags) + sizeof(UnsignedInt)
                + sizeof(UnsignedInt);

        static constexpr auto MAX_PAYLOAD_LENGTH = 64 * 1024 * 1024;

        Header();

        void Encode(char* buffer) const;
        void Decode(const char* buffer);

        [[nodiscard]] bool IsPayloadLengthValid() const;

    };
}
