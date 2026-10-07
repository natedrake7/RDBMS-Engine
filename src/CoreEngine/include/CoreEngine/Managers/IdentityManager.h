#pragma once
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/Guards/Mutex.h>

namespace CoreEngine{
    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager {
        Catalog::IdentityColumnsHeader header;
        mutable MultiThreading::Mutex mutex;

        std::atomic<BigInt> reservedUpTo;
        std::atomic<BigInt> counter;

        void ReserveBlock(const ::Memory::IAllocator* allocator, BigInt value);

    public:
        IdentityManager();

        void SetHeaderIds(Int tableId, Int columnId);
        void SetHeader(const Catalog::IdentityColumnsHeader& newHeader);
        [[nodiscard]] const Catalog::IdentityColumnsHeader& GetHeader() const;

        template<DataTypes::IsInteger T>
        [[nodiscard]] T Generate(const ::Memory::IAllocator* allocator);

        template<DataTypes::IsInteger T>
        [[nodiscard]] T ReserveRange(const ::Memory::IAllocator* allocator, T range);

        template<DataTypes::IsInteger T>
        [[nodiscard]] bool TryGenerate(const ::Memory::IAllocator* allocator, T& value);
        void UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] Int GetIncrement() const;
    };
}
