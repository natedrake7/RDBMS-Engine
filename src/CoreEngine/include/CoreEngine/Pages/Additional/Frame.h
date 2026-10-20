#pragma once
#include <atomic>

#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Guards/Mutex.h>

#include "CoreEngine/BufferPool/StorageLayout.h"
#include "CoreEngine/Pages/PageLayout.h"

namespace CoreEngine::Pages{
    struct PageHeader;

    inline static constexpr StorageLayout::frame_identity_t INVALID_FRAME_IDENTITY = std::numeric_limits<StorageLayout::frame_identity_t>::max();

    [[nodiscard]] inline constexpr StorageLayout::frame_identity_t FrameIdentity(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        return (static_cast<StorageLayout::frame_identity_t>(slotIndex) << 32) | pageId;
    }

    [[nodiscard]] inline constexpr buffer_pool_slot_t SlotOf(const StorageLayout::frame_identity_t identity){
        return static_cast<buffer_pool_slot_t>(identity >> 32);
    }

    [[nodiscard]] inline constexpr page_id_t PageOf(const StorageLayout::frame_identity_t identity){
        return static_cast<page_id_t>(identity & 0xFFFFFFFFull);
    }

    enum class FrameState: UnsignedTinyInt{
        Free = 0,
        Loading = 1,
        Ready = 2,
        Failed = 3
    };

    struct Frame{
        mutable MultiThreading::Mutex _latch;

        object_t* _data;

        std::atomic<StorageLayout::frame_identity_t> _identity;

        log_sequence_number_t logSequenceNumber;

        std::atomic<Int> _pinCount;

        std::atomic<FrameState> _state;
        std::atomic<PageLayout::PagePriority> _priority;
        std::atomic<bool> _hasSecondChance;
        std::atomic<bool> _isDirty;

        inline static constexpr Int EVICTING_PIN_COUNT = -1;

        Frame()
            :   _data(nullptr), _identity(INVALID_FRAME_IDENTITY),
                logSequenceNumber(0), _pinCount(0),
                _state(FrameState::Free), _priority(PageLayout::PagePriority::LOW),
                _hasSecondChance(true), _isDirty(false){}

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

        void SetPinCount(const Int pinCount){
            this->_pinCount.store(pinCount, std::memory_order_release);
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

        void SetIdentity(const buffer_pool_slot_t slotIndex, const page_id_t pageId){
            this->_identity.store(FrameIdentity(slotIndex, pageId), std::memory_order_release);
        }

        void SetIdentity(const StorageLayout::frame_identity_t identity){
            this->_identity.store(identity, std::memory_order_release);
        }

        [[nodiscard]] StorageLayout::frame_identity_t Identity() const{
            return this->_identity.load(std::memory_order_relaxed);
        }

        void SetPriority(const PageLayout::PagePriority priority){
            this->_priority.store(priority, std::memory_order_release);
        }

        [[nodiscard]] buffer_pool_slot_t SlotIndex() const{
            return SlotOf(this->_identity.load(std::memory_order_relaxed));
        }

        [[nodiscard]] page_id_t PageId() const{
            return PageOf(this->_identity.load(std::memory_order_relaxed));
        }

        void SetState(const FrameState state){
            this->_state.store(state, std::memory_order_release);
        }

        [[nodiscard]] FrameState State() const{
            return this->_state.load(std::memory_order_relaxed);
        }

        [[nodiscard]] bool StateEquals(const FrameState state) const{
            return this->_state.load(std::memory_order_relaxed) == state;
        }

        [[nodiscard]] bool PriorityEquals(const PageLayout::PagePriority priority) const{
            return this->_priority.load(std::memory_order_relaxed) == priority;
        }
    };
}
