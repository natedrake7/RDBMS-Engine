#include "../../include/Memory/PersistentAllocator.h"
#include "../../include/Managers/GlobalMemoryManager.h"
#include "../../include/Memory/Chunk.h"


namespace CoreEngine::Memory{
    UnsignedInt PersistentAllocator::NewChunkCapacity(const UnsignedInt size) const{
        if (size > Chunk::MAX_SIZE)
            return size;

        if (!this->_tail)
            return std::max(size, Chunk::DEFAULT_SIZE);  // e.g., 16 KB

        return std::min(this->_tail->_size * 2, Chunk::MAX_SIZE);
    }

    void PersistentAllocator::AllocateNewChunk(const UnsignedInt size) const{
        const auto newChunkSize = this->NewChunkCapacity(size);

        if(GlobalMemoryManager::Get().TryReserveForMisc(newChunkSize) == false){
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

    PersistentAllocator::PersistentAllocator(){
        this->_head = nullptr;
        this->_tail = nullptr;
    }

    PersistentAllocator::PersistentAllocator(const UnsignedInt size){
        this->_head = nullptr;
        this->_tail = nullptr;
        this->AllocateNewChunk(size);
    }

    PersistentAllocator::~PersistentAllocator(){
        this->PersistentAllocator::Release();
    }

    PersistentAllocator::PersistentAllocator(PersistentAllocator&& other) noexcept{
        this->_head = other._head;
        this->_tail = other._tail;

        other._head = nullptr;
        other._tail = nullptr;
    }

    PersistentAllocator& PersistentAllocator::operator=(PersistentAllocator&& other) noexcept{
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

    void* PersistentAllocator::AllocateRaw(const UnsignedInt size)const{
        if (this->_head == nullptr || this->_tail->_offset + size > this->_tail->_size)
            this->AllocateNewChunk(size);

        auto* ptr = this->_tail->_data + this->_tail->_offset;
        this->_tail->_offset += size;
        return ptr;
    }

    void PersistentAllocator::Release() const{
        if (this->_head == nullptr)
            return;

        auto* node = this->_head;
        Int totalMemoryFreed = 0;
        while(node != nullptr){
            auto* next = node->_next;
            totalMemoryFreed += node->_size;
            std::free(node);
            node = next;
        }

        this->_head = nullptr;
        this->_tail = nullptr;

        GlobalMemoryManager::Get().ReleaseMiscReservation(totalMemoryFreed);
    }

    void PersistentAllocator::Reset() const{
        auto* node = this->_head;
        while(node != nullptr){
            node->_offset = 0;
            node = node->_next;
        }
    }
}
