#include "../../../include/Pages/Additional/Frame.h"

namespace Pages{
    Frame::Frame(){
        this->data = nullptr;
        this->table = nullptr;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
    }

    Frame::Frame(object_t* data, const DatabaseEngine::StorageTypes::Table* table){
        this->data = data;
        this->table = table;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
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

        return *this;
    }
}