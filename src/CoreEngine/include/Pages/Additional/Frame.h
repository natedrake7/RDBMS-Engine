#pragma once
#include <atomic>

#include "../../DatabaseConstants.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataTypes/StringView.h"
#include "../../../../Systemic/include/Guards/Mutex.h"
#include "../../BufferPool/FileManager.h"

namespace CoreEngine::StorageTypes{
    class Table;
}

namespace Pages{
    using FrameId = Int;

    struct PageHeader;
    struct Frame{
        mutable MultiThreading::Mutex latch;

        object_t* _data;

        const CoreEngine::StorageTypes::Table* table;
        log_sequence_number_t logSequenceNumber;

        Storage::FileKey fileKey;
        std::atomic<Int> pinCount;

        std::atomic<Constants::PagePriority> priority;
        Constants::PageType type;
        bool hasSecondChance;
        bool isDirty;

        Frame();
        Frame(object_t* data, const CoreEngine::StorageTypes::Table* table);
        Frame& operator=(const Frame& other);

        [[nodiscard]] PageHeader* Header() const;
        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] bool TryPin();
        void Unpin();

        [[nodiscard]] bool TryClaimForEviction();
    };
}
