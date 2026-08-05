#include "../../include/Contexts/ExecutionContext.h"
#include "../../../Systemic/include/Coercions/Coercions.h"
#include "../../include/DataStorage/Row.h"
#include "../../../Systemic/include/DataTypes/Variable.h"

namespace CoreEngine{
    ExecutionContext::ExecutionContext(
        Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<DataTypes::String, Variable>* variables,
        const Int initialAllocatorSize
    )   :   snapshot(std::move(snapshot)),
            allocator(initialAllocatorSize),
            variables(variables),
            batchSize(batchSize){}

    ExecutionContext::ExecutionContext()
    : variables(nullptr), batchSize(0){}

    ExecutionContext::ExecutionContext(ExecutionContext&& other) noexcept
        :   snapshot(std::move(other.snapshot)),
            allocator(std::move(other.allocator)),
            variables(other.variables),
            batchSize(other.batchSize){
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

    const Variable* ExecutionContext::GetVariable(const DataTypes::String& name) const{
        return &this->variables->Get(name);
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

    void ExecutionContext::SetTable(
        const StorageTypes::Table* table,
        const UnsignedSmallInt slotIndex
    ){
        this->schema.tables[slotIndex] = table;
    }

    const StorageTypes::Table* ExecutionContext::GetTable(const UnsignedSmallInt index) const{
        return this->schema.tables[index];
    }

    const Storage::FileKey* ExecutionContext::GetFileKeys(const UnsignedSmallInt index) const{
        return this->schema.fileKeys[index];
    }

    Storage::FileKey ExecutionContext::GetFileKey(const UnsignedSmallInt slotIndex, const UnsignedSmallInt index) const{
        return this->schema.fileKeys[slotIndex][index];
    }

    void ExecutionContext::SetFileKey(
        const Storage::FileKey fileKey,
        const StorageTypes::RID::Source storageType,
        const UnsignedSmallInt slotIndex
    ){
        this->schema.fileKeys[slotIndex][static_cast<Int>(storageType)] = fileKey;
    }

    void ExecutionContext::SetScanHandle(const StorageTypes::RID* rids, const UnsignedInt size, const UnsignedSmallInt slotIndex){
        this->scanHandles[slotIndex] = ScanHandle(rids, size);
    }

    const ScanHandle& ExecutionContext::GetScanHandle(const UnsignedInt index) const{
        return this->scanHandles[index];
    }

    UnsignedInt ExecutionContext::GetScanHandleSize(const UnsignedInt index) const{
        return this->scanHandles[index].size;
    }

    const StorageTypes::RID* ExecutionContext::GetRIDPtr(const UnsignedInt slotIndex, const UnsignedInt ridIndex) const{
        return &this->scanHandles[slotIndex].rids[ridIndex];
    }

    StorageTypes::RID ExecutionContext::GetRID(const UnsignedInt slotIndex, const UnsignedInt ridIndex) const{
        return this->scanHandles[slotIndex].rids[ridIndex];
    }

    void ExecutionContext::ResetAllocator() const{
        this->allocator.Reset();
    }

    void ExecutionContext::ReleaseAllocator() const{
        this->allocator.Release();
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
