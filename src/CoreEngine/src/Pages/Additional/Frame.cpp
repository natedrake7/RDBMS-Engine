#include <CoreEngine/Pages/Additional/Frame.h>

#include <CoreEngine/Pages/PageView.h>

namespace Pages{
    Frame::Frame()
        :   _data(nullptr), logSequenceNumber(0),
            _pinCount(0), priority(Constants::PagePriority::LOW),
            hasSecondChance(true), isDirty(false){}

    Frame::Frame(object_t* data)
        :   _data(data), logSequenceNumber(0),
            _pinCount(0), priority(Constants::PagePriority::LOW),
            hasSecondChance(true), isDirty(false){}

    Frame& Frame::operator=(const Frame& other){
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->isDirty.store(other.isDirty.load(std::memory_order_relaxed), std::memory_order_relaxed);
        this->_pinCount.store(other._pinCount.load(std::memory_order_relaxed), std::memory_order_relaxed);
        this->priority.store(other.priority.load(std::memory_order_relaxed), std::memory_order_relaxed);
        this->hasSecondChance.store(other.hasSecondChance.load(std::memory_order_relaxed), std::memory_order_relaxed);
        this->logSequenceNumber = other.logSequenceNumber;

        return *this;
    }

    PageHeader* Frame::Header() const{
        return reinterpret_cast<PageHeader*>(this->_data);
    }

    bool Frame::IsValid() const{
        return this->_data != nullptr;
    }

    bool Frame::TryPin(){
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

    void Frame::Unpin(){
        this->_pinCount.fetch_sub(1, std::memory_order_release);
    }

    bool Frame::TryClaimForEviction(){
        Int expectedPinCount = 0;
        static constexpr Int EVICTING_PIN_COUNT = -1;
        return this->_pinCount.compare_exchange_strong(
                expectedPinCount, EVICTING_PIN_COUNT,
                std::memory_order_acquire, std::memory_order_relaxed
        );
    }
}
