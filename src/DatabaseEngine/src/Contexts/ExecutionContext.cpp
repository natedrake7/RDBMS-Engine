#include "../../include/Contexts/ExecutionContext.h"

namespace DatabaseEngine{
    ExecutionContext::ExecutionContext(
        const Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<std::string, Variable>& variables,
        const Int initialAllocatorSize
    ){
        this->snapshot = snapshot;
        this->batchSize = batchSize;
        this->variables = &variables;

        if (!GlobalMemoryManager::Get().TryReserveForExecution(initialAllocatorSize))
            throw std::bad_alloc();

        this->allocator = Memory::Allocator(initialAllocatorSize);
    }

    ExecutionContext::ExecutionContext(){
        this->batchSize = 0;
        this->variables = nullptr;
    }

    ExecutionContext::ExecutionContext(ExecutionContext&& other) noexcept{
        this->snapshot = other.snapshot;
        this->batchSize = other.batchSize;
        this->variables = other.variables;
        this->allocator = std::move(other.allocator);

        other.variables = nullptr;
    }

    ExecutionContext& ExecutionContext::operator=(ExecutionContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->snapshot = other.snapshot;
        this->batchSize = other.batchSize;
        this->variables = other.variables;
        this->allocator = std::move(other.allocator);

        other.variables = nullptr;

        return *this;
    }

    ExecutionContext::~ExecutionContext(){
        // GlobalMemoryManager::Get().ReleaseExecutionReservation(this->allocationSize);
    }

    void ExecutionContext::SetBatchSize(const Int size){
        this->batchSize = size;
    }

    const Memory::Allocator& ExecutionContext::GetAllocator() const{
        return this->allocator;
    }

    const Dictionary<std::string, Variable>* ExecutionContext::GetVariables() const{
        return this->variables;
    }

    Int ExecutionContext::GetBatchSize() const{
        return this->batchSize;
    }

    transaction_id_t ExecutionContext::GetCurrentTransactionId() const{
        return this->snapshot.transactionId;
    }

    const Snapshot& ExecutionContext::GetSnapshot() const{
        return this->snapshot;
    }

    void* ExecutionContext::Allocate(const Int size) const{
        if (!GlobalMemoryManager::Get().TryReserveForExecution(size))
            throw std::bad_alloc();

        return this->allocator.Allocate(size);
    }
}
