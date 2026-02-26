#include "../../include/Memory/Allocator.h"

#include "../../../DatabaseEngine/include/Managers/GlobalMemoryManager.h"

namespace Memory{
    UnsignedInt Allocator::NewChunkCapacity(const UnsignedInt size) const{
        if (size > Chunk::MAX_SIZE)
            return size;

        if (!this->_tail)
            return std::max(size, Chunk::DEFAULT_SIZE);  // e.g., 16 KB

        return std::min(this->_tail->_size * 2, Chunk::MAX_SIZE);
    }

    Allocator::Allocator(){
        this->_head = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + Chunk::DEFAULT_SIZE));
        this->_head->_size = Chunk::DEFAULT_SIZE;
        this->_head->_offset = 0;
        this->_tail = this->_head;
        this->_head->_next = nullptr;
    }

    Allocator::Allocator(const UnsignedInt capacity){
        this->_head = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + capacity));
        this->_head->_size = capacity;
        this->_head->_offset = 0;
        this->_tail = this->_head;
        this->_head->_next = nullptr;
    }

    Allocator::~Allocator(){
        this->Reset();
    }

    Allocator::Allocator(Allocator&& other) noexcept{
        this->_head = other._head;
        this->_tail = other._tail;

        other._head = nullptr;
        other._tail = nullptr;
    }

    Allocator& Allocator::operator=(Allocator&& other) noexcept{
        if (this == &other)
            return *this;

        this->_head = other._head;
        this->_tail = other._tail;

        other._head = nullptr;
        other._tail = nullptr;

        return *this;
    }

    void* Allocator::Allocate(const UnsignedInt size)const{
        if (this->_tail->_offset + size <= this->_tail->_size){
            auto* ptr = this->_tail->_data + this->_tail->_offset;
            this->_tail->_offset += size;
            return ptr;
        }

        const auto newChunkSize = this->NewChunkCapacity(size);
        auto* newChunk = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + newChunkSize));
        newChunk->_size = newChunkSize;
        newChunk->_offset = 0;

        newChunk->_next = nullptr;
        this->_tail->_next = newChunk;
        this->_tail = newChunk;

        auto* ptr = this->_tail->_data + this->_tail->_offset;
        this->_tail->_offset += size;
        return ptr;
    }

    void Allocator::Reset() const{
        auto* node = this->_head;
        while(node != nullptr){
            auto* next = node->_next;
            std::free(node);
            node = next;
        }
    }
}
