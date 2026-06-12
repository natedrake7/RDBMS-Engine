#include "../../include/Contexts/ExecutionContext.h"
#include "../../../Systemic/include/Coercions/Coercions.h"
#include "../../include/DataStorage/Row.h"

namespace CoreEngine{
    ExecutionContext::ExecutionContext(
        Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<DataTypes::String, Variable>& variables,
        const Constants::ExecutionMode mode,
        const Int initialAllocatorSize
    )   :   snapshot(std::move(snapshot)),
            allocator(initialAllocatorSize),
            variables(&variables),
            batchSize(batchSize), mode(mode){}

    ExecutionContext::ExecutionContext()
    : variables(nullptr), batchSize(0), mode(Constants::ExecutionMode::Row){}

    ExecutionContext::ExecutionContext(ExecutionContext&& other) noexcept
        :   snapshot(std::move(other.snapshot)),
            allocator(std::move(other.allocator)),
            variables(other.variables),
            batchSize(other.batchSize),
            mode(other.mode){
        other.variables = nullptr;
    }


    ExecutionContext& ExecutionContext::operator=(ExecutionContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->snapshot  = std::move(other.snapshot);
        this->batchSize = other.batchSize;
        this->variables = other.variables;
        this->allocator = std::move(other.allocator);
        this->mode = other.mode;

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

    Constants::ExecutionMode ExecutionContext::GetMode() const{
        return this->mode;
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
