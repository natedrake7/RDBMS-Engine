#pragma once
#include <atomic>
#include <string>

#include "../../BTree.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "BufferPool/StorageManager.h"

namespace Pages{
    struct IndexAllocationPageAdditionalHeader;
}

namespace DatabaseEngine::StorageTypes{
    class Table;
}

namespace Pages{
    struct Frame{
        mutable MultiThreading::ReadWriteMutex latch;
        DataTypes::StringView filename;

        union{
            IndexPageAdditionalHeader* indexHeaderPtr;
            IndexAllocationPageAdditionalHeader* allocationHeaderPtr;
        }additionalHeader;

        object_t* data;

        const DatabaseEngine::StorageTypes::Table* table;
        PageHeader* headerPtr;
        log_sequence_number_t logSequenceNumber;

        Storage::FileKey fileKey;
        std::atomic<int> pinCount;

        std::atomic<PagePriority> priority;
        PageType type;
        bool hasSecondChance;
        bool isDirty;

        Frame();
        Frame(object_t* data, const DatabaseEngine::StorageTypes::Table* table);
        Frame& operator=(const Frame& other);
    };
}
