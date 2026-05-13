#include "../../include/Memory/Allocator.h"
#include "../../include/Managers/GlobalMemoryManager.h"
#include "../../include/Memory/Chunk.h"
#include <iostream>
#include <ostream>


namespace CoreEngine::Memory{
    UnsignedInt Allocator::NewChunkCapacity(const UnsignedInt size) const{
        if (size > Chunk::MAX_SIZE)
            return size;

        if (!this->_tail)
            return std::max(size, Chunk::DEFAULT_SIZE);  // e.g., 16 KB

        return std::min(this->_tail->_size * 2, Chunk::MAX_SIZE);
    }

    void Allocator::AllocateNewChunk(const UnsignedInt size) const{
        const auto newChunkSize = this->NewChunkCapacity(size);

        if(GlobalMemoryManager::Get().TryReserveForExecution(newChunkSize) == false){
            throw std::bad_alloc();
        }

        auto* newChunk = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + newChunkSize));

        if (newChunk == nullptr)
            throw std::bad_alloc();

        newChunk->_size = newChunkSize;
        newChunk->_offset = 0;
        newChunk->_next = nullptr;

        if (this->_head == nullptr){
            this->_head = newChunk;
            this->_tail = newChunk;
            return;
        }

        this->_tail->_next = newChunk;
        this->_tail = newChunk;
    }

    void Allocator::TryFindNewChunk(const UnsignedInt size) const{
        auto* node = this->_tail->_next;
        while (node != nullptr && node->_offset + size > node->_size)
            node = node->_next;

        if (node != nullptr){
            this->_tail = node;
            return;
        }

        this->AllocateNewChunk(size);
    }

    Allocator::Allocator(){
        this->_head = nullptr;
        this->_tail = nullptr;
    }

    Allocator::Allocator(const UnsignedInt capacity){
        this->_head = nullptr;
        this->_tail = nullptr;
        this->AllocateNewChunk(capacity);
    }

    Allocator::~Allocator(){
        this->Allocator::Release();
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

        if (this->_head != nullptr)
            this->Release();

        this->_head = other._head;
        this->_tail = other._tail;

        other._head = nullptr;
        other._tail = nullptr;

        return *this;
    }

    void* Allocator::AllocateRaw(const UnsignedInt size)const{
        if (this->_head == nullptr)
            this->AllocateNewChunk(size);
        else if(this->_tail->_offset + size > this->_tail->_size)
            this->TryFindNewChunk(size);

        auto* ptr = this->_tail->_data + this->_tail->_offset;
        this->_tail->_offset += size;
        return ptr;
    }

    void Allocator::Release() const{
        auto* node = this->_head;
        Int totalMemoryFreed = 0;
        while(node != nullptr){
            auto* next = node->_next;
            totalMemoryFreed += static_cast<Int>(node->_size);
            std::free(node);
            node = next;
        }

        this->_head = nullptr;
        this->_tail = nullptr;

        GlobalMemoryManager::Get().ReleaseExecutionReservation(totalMemoryFreed);
    }

    void Allocator::Reset() const{
        auto* node = this->_head;
        while(node != nullptr){
            node->_offset = 0;
            node = node->_next;
        }

        this->_tail = this->_head;
    }

    bool Allocator::IsEmpty() const{
        return this->_head == nullptr;
    }
}
