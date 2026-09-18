#pragma once
#include <atomic>

#include "DataTypes/DataTypes.h"

struct CancellationToken{
    const std::atomic<request_id_t>* _activeRequestId;
    const std::atomic<request_id_t>* _cancelledRequestId;
    const std::atomic<bool>* _isClosing;

    CancellationToken()
        :   _activeRequestId(nullptr),
            _cancelledRequestId(nullptr),
            _isClosing(nullptr){}

    CancellationToken(
        const std::atomic<request_id_t>* activeRequestId,
        const std::atomic<request_id_t>* cancelledRequestId,
        const std::atomic<bool>* isCancelled
    ):  _activeRequestId(activeRequestId),
        _cancelledRequestId(cancelledRequestId),
        _isClosing(isCancelled){}

    CancellationToken(const CancellationToken&) = delete;
    CancellationToken(CancellationToken&& other) noexcept
        : _activeRequestId(other._activeRequestId),
          _cancelledRequestId(other._cancelledRequestId),
          _isClosing(other._isClosing){
            other._activeRequestId = nullptr;
            other._cancelledRequestId = nullptr;
            other._isClosing = nullptr;
    }

    CancellationToken& operator=(const CancellationToken&) = delete;
    CancellationToken& operator=(CancellationToken&& other) noexcept{
        if (this == &other)
            return *this;

        this->_activeRequestId = other._activeRequestId;
        this->_cancelledRequestId = other._cancelledRequestId;
        this->_isClosing = other._isClosing;

        other._activeRequestId = nullptr;
        other._cancelledRequestId = nullptr;
        other._isClosing = nullptr;
        return *this;
    }

    [[nodiscard]] bool IsCancelled() const{
        if (this->_isClosing == nullptr)
            return false;
        if (this->_isClosing->load(std::memory_order_relaxed))
            return true;

        const auto active = this->_activeRequestId->load(std::memory_order_relaxed);
        return active != 0
            && this->_cancelledRequestId->load(std::memory_order_relaxed) == active;
    }
};
