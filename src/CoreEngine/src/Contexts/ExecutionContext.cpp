#include "../../include/Contexts/ExecutionContext.h"

#include "Coercions.h"
#include "DataStorage/Row.h"

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->selectedRids[index] = static_cast<UnsignedInt*>(allocator->AllocateRaw(size * sizeof(UnsignedInt)));
        this->isIdentity = false;
    }

    void SelectionVector::AllocateNullMask(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->nullMask[index] = static_cast<UnsignedTinyInt*>(allocator->AllocateRaw(size));
        this->isIdentity = false;
    }

    ExecutionContext::ExecutionContext(
        const Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<DataTypes::String, Variable>& variables,
        const Int initialAllocatorSize
    )   : snapshot(snapshot),
          allocator(initialAllocatorSize),
          batchSize(batchSize){
        this->variables = &variables;
    }

    ExecutionContext::ExecutionContext()
    : variables(nullptr), batchSize(0){}

    ExecutionContext::ExecutionContext(ExecutionContext&& other) noexcept
        : snapshot(std::move(other.snapshot)),
          allocator(std::move(other.allocator)),
          variables(other.variables),
          batchSize(other.batchSize)
    {
        other.variables = nullptr;
    }


    ExecutionContext& ExecutionContext::operator=(ExecutionContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->snapshot  = std::move(other.snapshot);
        this->batchSize = other.batchSize;
        this->variables = other.variables;
        this->allocator = std::move(other.allocator);

        other.variables = nullptr;

        return *this;
    }

    ExecutionContext::~ExecutionContext() = default;

    void ExecutionContext::SetBatchSize(const Int size){
        this->batchSize = size;
    }

    const ::Memory::IAllocator* ExecutionContext::GetAllocator() const{
        return &this->allocator;
    }

    const Dictionary<DataTypes::String, Variable>* ExecutionContext::GetVariables() const{
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

    void ExecutionContext::AddTable(const StorageTypes::Table* table){
        this->schema.tables[this->schema.tableCount++] = table;
    }

    const StorageTypes::Table* ExecutionContext::GetTable(const UnsignedInt index) const{
        return this->schema.tables[index];
    }

    void ExecutionContext::AddScanHandle(const StorageTypes::RID* rids, const UnsignedInt size){
        this->scanContext.scanHandles[this->scanContext.scanHandleCount++] = ScanHandle(rids, size);
    }

    const ScanHandle& ExecutionContext::GetScanHandle(const UnsignedInt index) const{
        return this->scanContext.scanHandles[index];
    }

    const StorageTypes::RID* ExecutionContext::GetRid(const UnsignedInt scanHandleIndex, const UnsignedInt ridIndex) const{
        return &this->scanContext.scanHandles[scanHandleIndex].rids[ridIndex];
    }

    void ExecutionContext::ResetAllocator() const{
        this->allocator.Reset();
    }

    bool ExecutionContext::IsAllocatorEmpty() const{
        return this->allocator.IsEmpty();
    }

    void* ExecutionContext::Allocate(const Int size) const{
        return this->allocator.AllocateRaw(size);
    }

    ExecutionContext ExecutionContext::BaseContext(){
        return ExecutionContext();
    }
}
