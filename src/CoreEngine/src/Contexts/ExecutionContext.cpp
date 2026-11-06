#include <CoreEngine/Contexts/ExecutionContext.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/SystemDatabases/VersionDatabase.h>

#include "CoreEngine/DataStorage/Table.h"

namespace CoreEngine{
    ExecutionContext::ExecutionContext(
        Snapshot& snapshot,
        const Int32_t batchSize,
        const Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>>* variables,
        const Int32_t initialAllocatorSize
    )   :   _snapshot(std::move(snapshot)),
            _allocator(initialAllocatorSize),
            _variables(variables),
            _batchSize(batchSize){
    }

    ExecutionContext::ExecutionContext()
    : _variables(nullptr), _batchSize(0){}

    ExecutionContext::ExecutionContext(ExecutionContext&& other) noexcept
        :   _snapshot(std::move(other._snapshot)),
            _allocator(std::move(other._allocator)),
            _variables(other._variables),
            _batchSize(other._batchSize){
        other._variables = nullptr;
    }

    ExecutionContext& ExecutionContext::operator=(ExecutionContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->_snapshot  = std::move(other._snapshot);
        this->_batchSize = other._batchSize;
        this->_variables = other._variables;
        this->_allocator = std::move(other._allocator);

        other._variables = nullptr;

        return *this;
    }

    ExecutionContext::~ExecutionContext() = default;

    void ExecutionContext::SetBatchSize(const Int32_t size){
        this->_batchSize = size;
    }

    const ::Memory::IAllocator* ExecutionContext::GetAllocator() const{
        return &this->_allocator;
    }

    const BoundVariable* ExecutionContext::GetVariable(const DataTypes::StringView& name) const{
        if (this->_variables == nullptr)
            return nullptr;

        return this->_variables->Get(name).get();
    }

    Int32_t ExecutionContext::GetBatchSize() const{
        return this->_batchSize;
    }

    transaction_id_t ExecutionContext::GetCurrentTransactionId() const{
        return this->_snapshot.transactionId;
    }

    const Snapshot& ExecutionContext::GetSnapshot() const{
        return this->_snapshot;
    }

    void ExecutionContext::AttachCancellationToken(CancellationToken& cancellationToken){
        this->_cancellationToken = std::move(cancellationToken);
    }

    void ExecutionContext::SetTable(
        const StorageTypes::Table* table,
        const UInt16_t slotIndex
    ){
        this->_schema.tables[slotIndex] = table;
    }

    const StorageTypes::Table* ExecutionContext::GetTable(const UInt16_t index) const{
        return this->_schema.tables[index];
    }

    Accessors::PageAccessor ExecutionContext::GetPageAccessor(UInt16_t slotIndex) const{
        return this->_schema.tables[slotIndex]->GetPageAccessor();
    }

    void ExecutionContext::SetScanHandle(const StorageTypes::RID* rids, const UInt32_t size, const UInt16_t slotIndex){
        this->_scanHandles[slotIndex] = ScanHandle(rids, size);
    }

    const ScanHandle& ExecutionContext::GetScanHandle(const UInt32_t index) const{
        return this->_scanHandles[index];
    }

    UInt32_t ExecutionContext::GetScanHandleSize(const UInt32_t index) const{
        return this->_scanHandles[index].size;
    }

    const StorageTypes::RID* ExecutionContext::GetRIDs(const UInt32_t slotIndex) const{
        return this->_scanHandles[slotIndex].rids;
    }

    const StorageTypes::RID* ExecutionContext::GetRIDPtr(const UInt32_t slotIndex, const UInt32_t ridIndex) const{
        return &this->_scanHandles[slotIndex].rids[ridIndex];
    }

    StorageTypes::RID ExecutionContext::GetRID(const UInt32_t slotIndex, const UInt32_t ridIndex) const{
        return this->_scanHandles[slotIndex].rids[ridIndex];
    }

    void ExecutionContext::ResetAllocator() const{
        this->_allocator.Reset();
    }

    void ExecutionContext::ReleaseAllocator() const{
        this->_allocator.Release();
    }

    bool ExecutionContext::IsAllocatorEmpty() const{
        return this->_allocator.IsEmpty();
    }

    void* ExecutionContext::Allocate(const Int32_t size) const{
        return this->_allocator.AllocateRaw(size);
    }

    ExecutionContext ExecutionContext::BaseContext(){
        return ExecutionContext();
    }
}
