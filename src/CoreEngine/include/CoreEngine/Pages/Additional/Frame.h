#pragma once
#include <atomic>

#include <CoreEngine/DatabaseConstants.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Guards/Mutex.h>
#include <CoreEngine/BufferPool/FileManager.h>

namespace Pages{
    using FrameId = Int;
    struct PageHeader;

    struct Frame{
        mutable MultiThreading::Mutex _latch;

        object_t* _data;

        log_sequence_number_t logSequenceNumber;

        buffer_pool_index_t _slotIndex;
        std::atomic<Int> _pinCount;

        std::atomic<Constants::PagePriority> _priority;
        std::atomic<bool> _hasSecondChance;
        std::atomic<bool> _isDirty;

        inline static constexpr Int EVICTING_PIN_COUNT = -1;

        Frame()
            :   _data(nullptr), logSequenceNumber(0),
                _slotIndex(0), _pinCount(0),
                _priority(Constants::PagePriority::LOW), _hasSecondChance(true), _isDirty(false){}
        explicit Frame(object_t* data)
            :   _data(data), logSequenceNumber(0),
                _slotIndex(0), _pinCount(0),
                _priority(Constants::PagePriority::LOW), _hasSecondChance(true), _isDirty(false){}

        Frame& operator=(const Frame&) = delete;
        Frame(const Frame&) = delete;
        Frame& operator=(Frame&&) = delete;
        Frame(Frame&&) = delete;

        [[nodiscard]] PageHeader* Header() const{
            return reinterpret_cast<PageHeader*>(this->_data);
        }

        [[nodiscard]] bool IsValid()const{
            return this->_data != nullptr;
        }

        [[nodiscard]] bool TryPin(){
            auto currentPinCount = this->_pinCount.load(std::memory_order_relaxed);
            while (true){
                if (currentPinCount < 0)
                    return false;

                if (this->_pinCount.compare_exchange_weak(
                    currentPinCount, currentPinCount + 1,
                    std::memory_order_acquire, std::memory_order_relaxed
                )) return true;
            }
        }

        void Unpin(){
            this->_pinCount.fetch_sub(1, std::memory_order_release);
        }

        [[nodiscard]] bool IsDirty()const{
            return this->_isDirty.load(std::memory_order_acquire);
        }

        void MarkDirty(){
            this->_isDirty.store(true, std::memory_order_release);
        }

        void MarkFlushed(){
            this->_isDirty.store(false, std::memory_order_release);
        }

        void MarkHasSecondChance(){
            this->_hasSecondChance.store(true, std::memory_order_release);
        }

        void ClearHasSecondChance(){
            this->_hasSecondChance.store(false, std::memory_order_release);
        }

        [[nodiscard]] bool HasSecondChance() const{
            return this->_hasSecondChance.load(std::memory_order_acquire);
        }

        [[nodiscard]] bool TryClaimForEviction(){
            Int expectedPinCount = 0;
            return this->_pinCount.compare_exchange_strong(
                    expectedPinCount, EVICTING_PIN_COUNT,
                    std::memory_order_acquire, std::memory_order_relaxed
            );
        }
    };
}
