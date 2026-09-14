#include "../include/ClientConnection.h"

#include "Constants.h"
#include "Security/Session.h"

namespace Network{
    ClientConnection::ClientConnection(const socket_t socket)
        :   _socket(socket), _session(nullptr),
            _wantsWrite(false), _closing(false){}

    ClientConnection::~ClientConnection(){
        if (this->_socket != Constants::INVALID_FILE_DESCRIPTOR)
            Close(this->_socket);
    }

    socket_t ClientConnection::Socket() const{
        return this->_socket;
    }

    bool ClientConnection::IsAuthenticated() const{
        return this->_session != nullptr;
    }

    const DataTypes::Guid& ClientConnection::SessionId() const{
        if (!this->IsAuthenticated())
            throw std::runtime_error("Client is not authenticated");

        return this->_session->sessionId;
    }

    void ClientConnection::Bind(const Session* session){
        this->_session = session;
    }

    void ClientConnection::SendFrame(const Header& header, const char* payload, size_t payloadSize){
        std::lock_guard guard(this->_mutex);

        this->_write.Append(header, payload, payloadSize);
        const auto flushStatus = this->_write.Flush(this->_socket);
        if (flushStatus == Transport::IoStatus::Failed){
            this->MarkClosing();
            return;
        }

        this->_wantsWrite.store(flushStatus == Transport::IoStatus::WouldBlock, std::memory_order_relaxed);
    }

    Transport::IoStatus ClientConnection::FillReadBuffer(){
        return this->_read.Fill(this->_socket);
    }

    HeaderStatus ClientConnection::TryTakeHeader(Header& header, const char*& payload){
        return this->_read.TryTakeHeader(header, payload);
    }

    void ClientConnection::WaitForWriteDrain(){
        std::unique_lock guard(this->_mutex);
        this->_writeDrain.wait(guard, [this]{
            return this->_write.PendingSize() < HIGH_WATERMARK
                || this->_closing.load(std::memory_order_relaxed);
        });
    }

    Transport::IoStatus ClientConnection::DrainWrites(){
        std::unique_lock guard(this->_mutex);

        const auto flushStatus = this->_write.Flush(this->_socket);
        this->_wantsWrite.store(flushStatus == Transport::IoStatus::WouldBlock, std::memory_order_relaxed);

        const auto drained = this->_write.PendingSize() < LOW_WATERMARK;

        guard.unlock();                   // notify outside the lock: the woken thread
        if (drained)                      // would otherwise immediately block on it
            this->_writeDrain.notify_one();

        return flushStatus;
    }

    void ClientConnection::MarkClosing(){
        this->_closing.store(true, std::memory_order_relaxed);
        this->_writeDrain.notify_all();
    }

    bool ClientConnection::IsClosing() const{
        return this->_closing.load(std::memory_order_relaxed);
    }

    bool ClientConnection::WantsWrite() const{
        return this->_wantsWrite.load(std::memory_order_relaxed);
    }

    bool ClientConnection::TryBeginQuery(){
        auto expected = false;
        return this->_queryInFlight.compare_exchange_strong(expected, true);
    }

    void ClientConnection::EndQuery(){
        this->_queryInFlight.store(false, std::memory_order_relaxed);
    }
}
