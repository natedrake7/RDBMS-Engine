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
        mutable MultiThreading::Mutex latch;

        object_t* _data;

        // const CoreEngine::StorageTypes::Table* table;
        log_sequence_number_t logSequenceNumber;

        Storage::FileKey fileKey;
        std::atomic<Int> pinCount;

        std::atomic<Constants::PagePriority> priority;
        bool hasSecondChance;
        bool isDirty;

        Frame();
        explicit Frame(object_t* data);
        Frame& operator=(const Frame& other);

        [[nodiscard]] PageHeader* Header() const;
        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] bool TryPin();
        void Unpin();

        [[nodiscard]] bool TryClaimForEviction();
    };
}
