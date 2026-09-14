#pragma once
#include <atomic>
#include <condition_variable>

#include "../../Systemic/include/Network/Socket.h"
#include "../../Systemic/include/Network/Transport.h"

namespace Network{
    struct Session;

    class ClientConnection{
        socket_t _socket;
        Transport::ReadBuffer _read;
        Transport::WriteQueue _write;

        std::mutex _mutex;
        std::condition_variable _writeDrain;

        const Session* _session;
        std::atomic<bool> _wantsWrite;
        std::atomic<bool> _closing;
        std::atomic<bool> _queryInFlight;

        static constexpr size_t HIGH_WATERMARK = 4 * 1024 * 1024;
        static constexpr size_t LOW_WATERMARK  = 1 * 1024 * 1024;

        public:
            explicit ClientConnection(socket_t socket);
            ~ClientConnection();

            [[nodiscard]] socket_t Socket() const;
            [[nodiscard]] bool IsAuthenticated()const;
            [[nodiscard]] const DataTypes::Guid& SessionId()const;
            void Bind(const Session* session);
            void SendFrame(const Header& header, const char* payload, size_t payloadSize);

            [[nodiscard]] Transport::IoStatus FillReadBuffer();
            [[nodiscard]] HeaderStatus TryTakeHeader(Header& header, const char*& payload);


            void WaitForWriteDrain();
            [[nodiscard]] Transport::IoStatus DrainWrites();

            void MarkClosing();

            [[nodiscard]] bool IsClosing() const;
            [[nodiscard]] bool WantsWrite() const;

            [[nodiscard]] bool TryBeginQuery();
            void EndQuery();
    };
}
