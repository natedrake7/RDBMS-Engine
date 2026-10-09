#include <CoreEngine/Memory/Allocator.h>
#include <CoreEngine/Managers/GlobalMemoryManager.h>
#include <CoreEngine/Memory/Chunk.h>
#include <Systemic/Functions/MathFunctions.h>
#include <iostream>

namespace CoreEngine::Memory{
    UnsignedInt Allocator::NewChunkCapacity(const UnsignedInt size) const{
        if (size > Chunk::MAX_SIZE)
            return size;

        if (!this->_tail)
            return Math::Max(size, Chunk::DEFAULT_SIZE);  // e.g., 16 KB

        return Math::Max(size, Math::Min(this->_tail->_size * 2, Chunk::MAX_SIZE));
    }

    void Allocator::AllocateNewChunk(const UnsignedInt size, const std::size_t alignment) const{
        const auto newChunkSize = this->NewChunkCapacity(size + Chunk::MaxPadding(alignment));

        if(GlobalMemoryManager::Get().TryReserveForExecution(newChunkSize) == false)
            throw std::bad_alloc();

        auto* newChunk = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + newChunkSize));

        if (newChunk == nullptr){
            GlobalMemoryManager::Get().ReleaseExecutionReservation(newChunkSize);
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

        newChunk->_next = this->_tail->_next;
        this->_tail->_next = newChunk;
        this->_tail = newChunk;
    }

    void Allocator::TryFindNewChunk(const UnsignedInt size, const std::size_t alignment) const{
        auto* node = this->_tail->_next;
        while (node != nullptr && node->AlignedOffset(alignment) + size > node->_size)
            node = node->_next;

        if (node != nullptr){
            this->_tail = node;
            return;
        }

        this->AllocateNewChunk(size, alignment);
    }

    Allocator::Allocator()
        : _head(nullptr), _tail(nullptr){}

    Allocator::Allocator(const UnsignedInt capacity)
        : _head(nullptr), _tail(nullptr){
        this->AllocateNewChunk(capacity, ::Memory::DEFAULT_ALIGNMENT);
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

    void* Allocator::AllocateAligned(const UnsignedInt size, const std::size_t alignment)const{
        if (this->_head == nullptr)
            this->AllocateNewChunk(size, alignment);

        auto offSet = this->_tail->AlignedOffset(alignment);
        if(offSet + size > this->_tail->_size){
            this->TryFindNewChunk(size, alignment);
            offSet = this->_tail->AlignedOffset(alignment);
        }

        assert(offSet + size <= this->_tail->_size && "Allocator::AllocateAligned: Overflow on tail");

        this->_tail->_offset = offSet + size;
        return this->_tail->_data + offSet;
    }

    void Allocator::Release() const{
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

        GlobalMemoryManager::Get().ReleaseExecutionReservation(freedMemory);
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

    ::Memory::AllocationStep Allocator::RecordAllocationStart() const{
        if (this->_tail == nullptr)
            this->AllocateNewChunk(0, ::Memory::DEFAULT_ALIGNMENT);

        return ::Memory::AllocationStep(this->_tail, this->_tail->_offset);
    }

    void Allocator::ReleaseFromAllocationStep(::Memory::AllocationStep& step) const{
        if (step._chunkAddress == nullptr)
            return;

        auto* currentChunk = static_cast<Chunk*>(step._chunkAddress);
        this->_tail = currentChunk;

        currentChunk->_offset = step._chunkOffset;
        currentChunk = currentChunk->_next;

        step._chunkAddress = nullptr;

        while (currentChunk != nullptr){
            currentChunk->_offset = 0;
            currentChunk = currentChunk->_next;
        }
    }
}
