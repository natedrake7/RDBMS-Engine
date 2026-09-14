#pragma once
#include "../DataTypes/DataTypes.h"
#include <vector>

#include "Header.h"
#include "Socket.h"

namespace Network{
    struct Header;
}

namespace Network::Transport{
    enum class IoStatus : UnsignedTinyInt{
        Ok = 0,
        WouldBlock = 1,
        Closed = 2,
        Failed = 3
    };

    class ReadBuffer{
        std::vector<char> _buffer;
        size_t _consumed;

        static constexpr auto READ_CHUNK = 64 * 1024;
        static constexpr auto COMPACT_THRESHOLD = 64 * 1024;
        static constexpr auto MAX_BUFFER_SIZE = 80 * 1024 * 1024;

        void Compact();

        public:
            ReadBuffer();

            void Append(const char* bytes, size_t size);
            [[nodiscard]] IoStatus Fill(socket_t socket);


            [[nodiscard]] HeaderStatus TryTakeHeader(Header& header, const char*& payload);
            [[nodiscard]] bool HasBufferedBytes()const;
    };

    class WriteQueue{
        std::vector<char> _buffer;
        size_t _sent;

        static constexpr auto COMPACT_THRESHOLD = 64 * 1024;

        public:
            WriteQueue();

            void Append(const Header& header, const char* payload, size_t payloadSize);

            [[nodiscard]] IoStatus Flush(socket_t socket);

            [[nodiscard]] size_t PendingSize()const;
    };
}
