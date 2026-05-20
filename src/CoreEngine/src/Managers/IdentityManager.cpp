#include "../../include/Managers/IdentityManager.h"

#include "../../include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"


namespace CoreEngine::StorageTypes {
    void IdentityManager::UpdateMasterDb(const ::Memory::IAllocator* allocator, const BigInt value){
        MultiThreading::WriterGuard guard(&this->mutex);

        if (value < this->startingValue + this->header.cacheBlock)
            return;

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->header.tableId,
            this->header.columnId,
            value + this->header.increment
        );

        this->header.lastValue += this->header.increment;
    }

    IdentityManager::IdentityManager() {
        this->startingValue = 0;
        this->counter = 0;
    }

    IdentityManager::~IdentityManager() = default;

    void IdentityManager::SetHeaderIds(const Int tableId, const Int columnId){
        this->header.tableId = tableId;
        this->header.columnId = columnId;
    }

    void IdentityManager::SetHeader(const Headers::IdentityColumnsHeader &newHeader){
        this->header = newHeader;
        this->startingValue = this->header.lastValue;
        this->counter.store(this->startingValue, std::memory_order_relaxed);
    }

    const Headers::IdentityColumnsHeader& IdentityManager::GetHeader() const{
        return this->header;
    }

    BigInt IdentityManager::Generate(const ::Memory::IAllocator* allocator){
        const auto value = this->counter.fetch_add(this->header.increment, std::memory_order_relaxed);

        if (value < this->startingValue + this->header.cacheBlock)
            return value;

        this->UpdateMasterDb(allocator, value);

        return value;
    }

    bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, BigInt& value){
        if (this->header.columnId == INVALID_COLUMN_ID)
            return false;

        value = this->Generate(allocator);

        return true;
    }

    void IdentityManager::UpdateMasterDb(const ::Memory::IAllocator* allocator) const{
        if (this->header.columnId == INVALID_COLUMN_ID) return;

        MultiThreading::WriterGuard guard(&this->mutex);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->header.tableId,
            this->header.columnId,
            this->header.lastValue
        );
    }

    bool IdentityManager::IsValid() const{ return this->header.columnId != INVALID_COLUMN_ID; }
}
