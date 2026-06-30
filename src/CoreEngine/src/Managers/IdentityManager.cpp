#include "../../include/Managers/IdentityManager.h"

#include "../../include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

namespace CoreEngine::StorageTypes {
    void IdentityManager::ReserveBlock(const ::Memory::IAllocator* allocator, const BigInt value){
        MultiThreading::WriterGuard guard(&this->mutex);

        auto ceiling = this->reservedUpTo.load(std::memory_order_relaxed);
        if (value < ceiling)
            return;

        do{
          ceiling += this->header.cacheBlock;
        }while (value >= ceiling);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->header.tableId,
            this->header.columnId,
            ceiling
        );

        this->reservedUpTo.store(ceiling, std::memory_order_relaxed);
    }

    IdentityManager::IdentityManager()
        : reservedUpTo(0), counter(0){}

    void IdentityManager::SetHeaderIds(const Int tableId, const Int columnId){
        this->header.tableId = tableId;
        this->header.columnId = columnId;
    }

    void IdentityManager::SetHeader(const Headers::IdentityColumnsHeader &newHeader){
        this->header = newHeader;
        this->reservedUpTo.store(this->header.lastValue, std::memory_order_relaxed);
        // this->reservedUpTo.store(this->header.lastValue + this->header.cacheBlock, std::memory_order_relaxed);
        this->counter.store(this->header.lastValue, std::memory_order_relaxed);
    }

    const Headers::IdentityColumnsHeader& IdentityManager::GetHeader() const{
        return this->header;
    }

    BigInt IdentityManager::Generate(const ::Memory::IAllocator* allocator){
        const auto value = this->counter.fetch_add(this->header.increment, std::memory_order_relaxed);

        if (value >= this->reservedUpTo.load(std::memory_order_relaxed))
            this->ReserveBlock(allocator, value);

        return value;
    }

    bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, BigInt& value){
        if (this->header.columnId == INVALID_COLUMN_ID)
            return false;

        value = this->Generate(allocator);

        return true;
    }

    void IdentityManager::UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const{
        if (this->header.columnId == INVALID_COLUMN_ID)
            return;

        MultiThreading::WriterGuard guard(&this->mutex);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->header.tableId,
            this->header.columnId,
            this->counter.load(std::memory_order_relaxed)
        );
    }

    bool IdentityManager::IsValid() const{ return this->header.columnId != INVALID_COLUMN_ID; }
}
