#include "../../include/Contexts/ExecutionContext.h"
#include "../../include/DataStorage/Row.h"
#include "SystemDatabases/VersionDatabase.h"

namespace CoreEngine{
    ExecutionContext::ExecutionContext(
        Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>>* variables,
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

    const BoundVariable* ExecutionContext::GetVariable(const DataTypes::StringView& name) const{
        if (this->variables == nullptr)
            return nullptr;

        return this->variables->Get(name).get();
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

    DataStructures::StaticArray<Storage::FileKey, StorageTypes::RID::Count> ExecutionContext::GetFileKeys(const UnsignedSmallInt index) const{
        DataStructures::StaticArray<Storage::FileKey, StorageTypes::RID::Count> fileKeys;
        fileKeys[StorageTypes::RID::Table] = this->schema.fileKeys[index];
        fileKeys[StorageTypes::RID::Version] = VersionDatabase::Get().GetDataFileKey();

        return fileKeys;
    }

    Storage::FileKey ExecutionContext::GetFileKey(const UnsignedSmallInt slotIndex, const UnsignedSmallInt index) const{
        return this->schema.fileKeys[slotIndex];
    }

    void ExecutionContext::SetFileKey(
        const Storage::FileKey fileKey,
        const UnsignedSmallInt slotIndex
    ){
        this->schema.fileKeys[slotIndex] = fileKey;
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

    const StorageTypes::RID* ExecutionContext::GetRIDs(const UnsignedInt slotIndex) const{
        return this->scanHandles[slotIndex].rids;
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
