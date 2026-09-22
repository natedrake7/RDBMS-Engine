#include "../../include/Contexts/ExecutionContext.h"
#include "../../include/DataStorage/Row/Row.h"
#include "SystemDatabases/VersionDatabase.h"

namespace CoreEngine{
    ExecutionContext::ExecutionContext(
        Snapshot& snapshot,
        const Int batchSize,
        const Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>>* variables,
        const Int initialAllocatorSize
    )   :   _snapshot(std::move(snapshot)),
            _allocator(initialAllocatorSize),
            _variables(variables),
            _batchSize(batchSize){}

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

    void ExecutionContext::SetBatchSize(const Int size){
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

    Int ExecutionContext::GetBatchSize() const{
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
        const UnsignedSmallInt slotIndex
    ){
        this->_schema.tables[slotIndex] = table;
    }

    const StorageTypes::Table* ExecutionContext::GetTable(const UnsignedSmallInt index) const{
        return this->_schema.tables[index];
    }

    DataStructures::StaticArray<Storage::FileKey, StorageTypes::RID::Count> ExecutionContext::GetFileKeys(const UnsignedSmallInt index) const{
        DataStructures::StaticArray<Storage::FileKey, StorageTypes::RID::Count> fileKeys;
        fileKeys[StorageTypes::RID::Table] = this->_schema.fileKeys[index];
        fileKeys[StorageTypes::RID::Version] = VersionDatabase::Get().GetDataFileKey();

        return fileKeys;
    }

    Storage::FileKey ExecutionContext::GetFileKey(const UnsignedSmallInt slotIndex, const UnsignedSmallInt index) const{
        return this->_schema.fileKeys[slotIndex];
    }

    void ExecutionContext::SetFileKey(
        const Storage::FileKey fileKey,
        const UnsignedSmallInt slotIndex
    ){
        this->_schema.fileKeys[slotIndex] = fileKey;
    }

    void ExecutionContext::SetScanHandle(const StorageTypes::RID* rids, const UnsignedInt size, const UnsignedSmallInt slotIndex){
        this->_scanHandles[slotIndex] = ScanHandle(rids, size);
    }

    const ScanHandle& ExecutionContext::GetScanHandle(const UnsignedInt index) const{
        return this->_scanHandles[index];
    }

    UnsignedInt ExecutionContext::GetScanHandleSize(const UnsignedInt index) const{
        return this->_scanHandles[index].size;
    }

    const StorageTypes::RID* ExecutionContext::GetRIDs(const UnsignedInt slotIndex) const{
        return this->_scanHandles[slotIndex].rids;
    }

    const StorageTypes::RID* ExecutionContext::GetRIDPtr(const UnsignedInt slotIndex, const UnsignedInt ridIndex) const{
        return &this->_scanHandles[slotIndex].rids[ridIndex];
    }

    StorageTypes::RID ExecutionContext::GetRID(const UnsignedInt slotIndex, const UnsignedInt ridIndex) const{
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

    void* ExecutionContext::Allocate(const Int size) const{
        return this->_allocator.AllocateRaw(size);
    }

    ExecutionContext ExecutionContext::BaseContext(){
        return ExecutionContext();
    }
}
