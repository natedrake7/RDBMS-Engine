#pragma once
#include <atomic>

#include <CoreEngine/DatabaseConstants.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Guards/Mutex.h>
#include <CoreEngine/BufferPool/FileManager.h>

namespace CoreEngine::StorageTypes{
    class Table;
}

namespace Pages{
    using FrameId = Int;
    struct PageHeader;

    struct Frame{
        mutable MultiThreading::Mutex _latch;

        object_t* _data;

        log_sequence_number_t logSequenceNumber;

        Storage::FileKey _fileKey;
        std::atomic<Int> _pinCount;

        std::atomic<Constants::PagePriority> priority;
        std::atomic<bool> hasSecondChance;
        std::atomic<bool> isDirty;

        Frame();
        explicit Frame(object_t* data);
        Frame& operator=(const Frame& other);

        [[nodiscard]] PageHeader* Header() const;
        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] bool TryPin();
        void Unpin();

        [[nodiscard]] bool IsDirty()const{
            return this->isDirty.load(std::memory_order_acquire);
        }

        void MarkDirty(){
            this->isDirty.store(true, std::memory_order_release);
        }

        void MarkFlushed(){
            this->isDirty.store(false, std::memory_order_release);
        }

        [[nodiscard]] bool TryClaimForEviction();
    };
}
