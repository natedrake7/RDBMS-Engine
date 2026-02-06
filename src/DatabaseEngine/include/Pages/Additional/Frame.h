#pragma once
#include <atomic>
#include <string>

#include "../../BTree.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

namespace Pages
{
    struct IndexAllocationPageAdditionalHeader;
}

namespace DatabaseEngine::StorageTypes{
    class Table;
}

namespace Pages{
    struct Frame{
        bool isDirty;
        std::atomic<int> pinCount;
        std::atomic<PagePriority> priority;
        bool hasSecondChance;

        mutable MultiThreading::ReadWriteMutex latch;

        log_sequence_number_t logSequenceNumber;

        std::string filename;
        const DatabaseEngine::StorageTypes::Table* table;

        PageHeader* headerPtr;
        PageType type;

        union{
            IndexPageAdditionalHeader* indexHeaderPtr;
            IndexAllocationPageAdditionalHeader* allocationHeaderPtr;
        }additionalHeader;

        object_t* data;

        Frame();
        Frame(object_t* data, const DatabaseEngine::StorageTypes::Table* table);
        Frame& operator=(const Frame& other);
    };
}
