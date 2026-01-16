#include "../../include/Memory/Allocator.h"

namespace Memory{
    Allocator::Allocator(const BigInt& capacity, const AllocationType& allocationType){
        this->_capacity = capacity;
        this->_offset = 0;
        this->_buffer = static_cast<object_t*>(std::malloc(capacity));
        this->_allocationType = allocationType;
    }

    Allocator::~Allocator(){
        if (this->_allocationType == AllocationType::Persistent){

            if (this->_offset < this->_capacity){
                this->_buffer += this->_offset;
                std::free(this->_buffer);
            }

            return;
        }

        std::free(this->_buffer);
    }

    void* Allocator::Allocate(const BigInt& size){
        //reallocate memory (expensive, should be avoided)
        if (this->_offset + size > this->_capacity)
        {
            std::realloc(this->_buffer, this->_capacity + size + (this->_capacity / 2));
            this->_capacity += size + (this->_capacity / 2);
        }

        void* ptr = this->_buffer + this->_offset;
        this->_offset += size;

        return ptr;
    }

    void Allocator::Reset(){
        this->_offset = 0;
    }
}
