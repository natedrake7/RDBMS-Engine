#include "../../include/Network/Transport.h"

#include "Network/Header.h"

namespace Network::Transport{
    void ReadBuffer::Compact(){
        if (this->_consumed == 0)
            return;

        if (this->_consumed < COMPACT_THRESHOLD && this->_consumed < this->_buffer.size())
            return;

        this->_buffer.erase(this->_buffer.begin(), this->_buffer.begin() + static_cast<BigInt>(this->_consumed));
        this->_consumed = 0;
    }

    ReadBuffer::ReadBuffer()
        : _consumed(0){}

    void ReadBuffer::Append(const char* bytes, const size_t size){
        this->_buffer.insert(this->_buffer.end(), bytes, bytes + size);
    }

    IoStatus ReadBuffer::Fill(const socket_t socket){
        this->Compact();

        char chunk[READ_CHUNK];
        while (true){
            if (this->_buffer.size() >= MAX_BUFFER_SIZE)
                return IoStatus::Failed;

            const auto bytesRead = recv(socket, chunk, READ_CHUNK, 0);
            if (bytesRead > 0){
                this->_buffer.insert(this->_buffer.end(), chunk, chunk + bytesRead);
                continue;
            }

            if (bytesRead == 0)
                return IoStatus::Closed;

            if (IsInterrupted())
                continue;

            return WouldBlock() ? IoStatus::WouldBlock : IoStatus::Failed;
        }
    }

    HeaderStatus ReadBuffer::TryTakeHeader(Header& header, const char*& payload){
        const auto availableBytes = this->_buffer.size() - this->_consumed;
        if (availableBytes < Header::SIZE)
            return HeaderStatus::Incomplete;

        header.Decode(this->_buffer.data() + this->_consumed);

        if (!header.IsPayloadLengthValid())
            return HeaderStatus::Malformed;

        const auto requestSize = Header::SIZE + header._payloadLength;
        if (availableBytes < requestSize)
            return HeaderStatus::Incomplete;

        payload = this->_buffer.data() + this->_consumed + Header::SIZE;
        this->_consumed += requestSize;
        return HeaderStatus::Ready;

    }

    bool ReadBuffer::HasBufferedBytes() const{
        return this->_consumed < this->_buffer.size();
    }

    WriteQueue::WriteQueue()
        : _sent(0){}

    void WriteQueue::Append(const Header& header, const char* payload, const size_t payloadSize){
        const auto base = this->_buffer.size();
        this->_buffer.resize(base + Header::SIZE + payloadSize);
        header.Encode(this->_buffer.data() + base);

        if (payloadSize > 0)
            std::memcpy(this->_buffer.data() + base + Header::SIZE, payload, payloadSize);
    }

    IoStatus WriteQueue::Flush(socket_t socket){
        while (this->_sent < this->_buffer.size()){
            const auto bytesSent = send(
                socket,
                this->_buffer.data() + this->_sent,
                static_cast<Int>(this->_buffer.size() - this->_sent), 0
            );

            if (bytesSent > 0){
                this->_sent += bytesSent;
                continue;
            }

            if (IsInterrupted())
                continue;

            if (WouldBlock()){
                if (this->_sent >= COMPACT_THRESHOLD){
                    this->_buffer.erase(this->_buffer.begin(), this->_buffer.begin() + static_cast<Int>(this->_sent));
                    this->_sent = 0;
                }

                return IoStatus::WouldBlock;
            }

            return IoStatus::Failed;
        }

        this->_buffer.clear();
        this->_sent = 0;
        return IoStatus::Ok;
    }

    size_t WriteQueue::PendingSize() const{
        return this->_buffer.size() - this->_sent;
    }

    IoStatus SendAll(const socket_t socket, const char* buffer, const size_t size){
        size_t sent = 0;
        while (sent < size){
            const auto bytesSent = send(socket, buffer + sent, size - sent, SEND_FLAGS);

            if (bytesSent > 0){
                sent += bytesSent;
                continue;
            }

            if (IsInterrupted())
                continue;

            return IoStatus::Failed;
        }

        return IoStatus::Ok;
    }

    IoStatus ReceiveExact(const socket_t socket, char* buffer, const size_t size){
        size_t received = 0;
        while (received < size){
            const auto bytesReceived = recv(socket, buffer + received, size - received, 0);

            if (bytesReceived > 0){
                received += bytesReceived;
                continue;
            }

            if (bytesReceived == 0)
                return IoStatus::Closed;

            if (IsInterrupted())
                continue;

            return IoStatus::Failed;
        }

        return IoStatus::Ok;
    }
}
