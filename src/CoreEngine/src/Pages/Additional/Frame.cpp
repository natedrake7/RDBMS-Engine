#include "../../../include/Pages/Additional/Frame.h"

namespace Pages{
    Frame::Frame(){
        this->_data = nullptr;
        this->table = nullptr;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = Constants::PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->type = Constants::PageType::DATA;
    }

    Frame::Frame(object_t* data, const CoreEngine::StorageTypes::Table* table){
        this->_data = data;
        this->table = table;
        this->isDirty = false;
        this->pinCount = 0;
        this->priority = Constants::PagePriority::LOW;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->type = Constants::PageType::DATA;
    }

    Frame& Frame::operator=(const Frame& other){
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->table = other.table;
        this->isDirty = other.isDirty;
        this->pinCount = other.pinCount.load();
        this->priority = other.priority.load();
        this->hasSecondChance = other.hasSecondChance;
        this->logSequenceNumber = other.logSequenceNumber;
        this->type = other.type;

        return *this;
    }

    PageHeader* Frame::Header() const{
        return reinterpret_cast<PageHeader*>(this->_data);
    }

    bool Frame::IsValid() const{
        return this->_data != nullptr;
    }
}
