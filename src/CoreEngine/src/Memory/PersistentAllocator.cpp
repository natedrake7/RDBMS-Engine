#include <CoreEngine/Memory/PersistentAllocator.h>
#include <CoreEngine/Managers/GlobalMemoryManager.h>
#include <CoreEngine/Memory/Chunk.h>
#include <Systemic/Functions/MathFunctions.h>
#include <cassert>
#include <stdexcept>


namespace CoreEngine::Memory{
    UnsignedInt PersistentAllocator::NewChunkCapacity(const UnsignedInt size) const{
        if (size > Chunk::MAX_SIZE)
            return size;

        if (!this->_tail)
            return Math::Max(size, Chunk::DEFAULT_SIZE);

        return Math::Max(size, Math::Min(this->_tail->_size * 2, Chunk::MAX_SIZE));
    }

    void PersistentAllocator::AllocateNewChunk(const UnsignedInt size, const std::size_t alignment) const{
        const auto newChunkSize = this->NewChunkCapacity(size + Chunk::MaxPadding(alignment));

        if(GlobalMemoryManager::Get().TryReserveForMisc(newChunkSize) == false)
            throw std::bad_alloc();

        auto* newChunk = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + newChunkSize));
        if (newChunk == nullptr){
            GlobalMemoryManager::Get().ReleaseMiscReservation(newChunkSize);
            throw std::bad_alloc();
        }

        newChunk->_size = newChunkSize;
        newChunk->_offset = 0;
        newChunk->_next = nullptr;

        if (this->_head == nullptr){
            this->_head = newChunk;
            this->_tail = newChunk;
            return;
        }

        // insert after the tail: chunks behind it (kept by Reset) stay reachable and are freed by Release
        newChunk->_next = this->_tail->_next;
        this->_tail->_next = newChunk;
        this->_tail = newChunk;
    }

    void PersistentAllocator::TryFindNewChunk(const UnsignedInt size, const std::size_t alignment) const{
        auto* node = this->_tail->_next;
        while (node != nullptr && node->AlignedOffset(alignment) + size > node->_size)
            node = node->_next;

        if (node != nullptr){
            this->_tail = node;
            return;
        }

        this->AllocateNewChunk(size, alignment);
    }

    PersistentAllocator::PersistentAllocator()
        : _head(nullptr), _tail(nullptr){}

    PersistentAllocator::PersistentAllocator(const UnsignedInt size)
        : _head(nullptr), _tail(nullptr){
        this->AllocateNewChunk(size, ::Memory::DEFAULT_ALIGNMENT);
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

    void* PersistentAllocator::AllocateAligned(const UnsignedInt size, const std::size_t alignment)const{
        if (this->_head == nullptr)
            this->AllocateNewChunk(size, alignment);

        auto offSet = this->_tail->AlignedOffset(alignment);
        if(offSet + size > this->_tail->_size){
            this->TryFindNewChunk(size, alignment);
            offSet = this->_tail->AlignedOffset(alignment);
        }

        assert(offSet + size <= this->_tail->_size && "PersistentAllocator::AllocateAligned: Overflow on tail");

        this->_tail->_offset = offSet + size;
        return this->_tail->_data + offSet;
    }

    void PersistentAllocator::Release() const{
        if (this->_head == nullptr)
            return;

        auto* node = this->_head;
        UnsignedBigInt freedMemory = 0;
        while(node != nullptr){
            auto* next = node->_next;
            freedMemory += node->_size;
            std::free(node);
            node = next;
        }

        this->_head = nullptr;
        this->_tail = nullptr;

        GlobalMemoryManager::Get().ReleaseMiscReservation(freedMemory);
    }

    void PersistentAllocator::Reset() const{
        auto* node = this->_head;
        while(node != nullptr){
            node->_offset = 0;
            node = node->_next;
        }

        this->_tail = this->_head;
    }

    ::Memory::AllocationStep PersistentAllocator::RecordAllocationStart() const{
        throw std::logic_error("PersistentAllocator::RecordAllocationStart Not implemented");
    }

    void PersistentAllocator::ReleaseFromAllocationStep(::Memory::AllocationStep& step) const{
        throw std::logic_error("PersistentAllocator::ReleaseFromAllocationStep Not implemented");
    }
}
