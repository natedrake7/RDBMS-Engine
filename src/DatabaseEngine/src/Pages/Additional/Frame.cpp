#include "../../../include/Pages/Additional/Frame.h"

namespace Pages{
    Frame::Frame(){
        this->data = nullptr;
        this->table = nullptr;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = Constants::PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->headerPtr = nullptr;
        this->type = Constants::PageType::DATA;
        this->additionalHeader.indexHeaderPtr = nullptr;
        this->additionalHeader.allocationHeaderPtr = nullptr;
    }

    Frame::Frame(object_t* data, const DatabaseEngine::StorageTypes::Table* table){
        this->data = data;
        this->table = table;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = Constants::PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->headerPtr = reinterpret_cast<PageHeader*>(data);
        this->type = Constants::PageType::DATA;
        this->additionalHeader.indexHeaderPtr = nullptr;
        this->additionalHeader.allocationHeaderPtr = nullptr;
    }

    Frame& Frame::operator=(const Frame& other){
        if (this == &other)
            return *this;

        this->data = other.data;
        this->table = other.table;
        this->isDirty = other.isDirty;
        this->pinCount = other.pinCount.load();
        this->priority = other.priority.load();
        this->hasSecondChance = other.hasSecondChance;
        this->logSequenceNumber = other.logSequenceNumber;
        this->type = other.type;
        this->headerPtr = other.headerPtr;
        this->additionalHeader.indexHeaderPtr = other.additionalHeader.indexHeaderPtr;
        this->additionalHeader.allocationHeaderPtr = other.additionalHeader.allocationHeaderPtr;

        return *this;
    }
}
