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

    bool Frame::TryPin(){
        auto currentPinCount = this->pinCount.load(std::memory_order_relaxed);
        while (true){
            if (currentPinCount < 0)
                return false;

            if (this->pinCount.compare_exchange_weak(
                currentPinCount, currentPinCount + 1,
                std::memory_order_acquire, std::memory_order_relaxed
            )) return true;
        }
    }

    void Frame::Unpin(){
        this->pinCount.fetch_sub(1, std::memory_order_release);
    }

    bool Frame::TryClaimForEviction(){
        Int expectedPinCount = 0;
        static constexpr Int EVICTING_PIN_COUNT = -1;
        return this->pinCount.compare_exchange_strong(
                expectedPinCount, EVICTING_PIN_COUNT,
                std::memory_order_acquire, std::memory_order_relaxed
        );
    }
}
