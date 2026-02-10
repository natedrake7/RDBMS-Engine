#include "../../include/Memory/Allocator.h"

namespace Memory{
    Allocator::Allocator(const UnsignedInt capacity){
        this->_capacity = capacity;
        this->_offset = 0;
        this->_buffer = static_cast<object_t*>(std::malloc(capacity));
    }

    Allocator::~Allocator(){
        std::free(this->_buffer);
    }

    Allocator::Allocator(Allocator&& other) noexcept{
        this->_capacity = other._capacity;
        this->_offset = other._offset;
        this->_buffer = other._buffer;

        other._buffer = nullptr;
    }

    Allocator& Allocator::operator=(Allocator&& other) noexcept{
        if (this == &other)
            return *this;

        this->_capacity = other._capacity;
        this->_offset = other._offset;
        this->_buffer = other._buffer;

        other._buffer = nullptr;

        return *this;
    }

    void* Allocator::Allocate(const UnsignedInt size)const{
        //reallocate memory (expensive, should be avoided)
        //calculate new capacity
        const auto isReallocationNeeded = this->_offset + size > this->_capacity;
        while(this->_offset + size > this->_capacity)
            this->_capacity += this->_capacity / 2;

        if (isReallocationNeeded)
            this->_buffer = static_cast<object_t*>(std::realloc(this->_buffer, this->_capacity));

        auto* ptr = this->_buffer + this->_offset;
        this->_offset += size;

        return ptr;
    }

    void Allocator::Reset() const{
        this->_offset = 0;
    }
}
