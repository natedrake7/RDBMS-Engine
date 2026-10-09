#pragma once
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/Guards/Mutex.h>

namespace CoreEngine{
    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager {
        mutable MultiThreading::Mutex mutex;

        const Schemas::IdentitySchema* _schema;


        std::atomic<BigInt> _reservedUpTo;
        std::atomic<BigInt> _counter;

        Int _tableId;
        Int _columnId;

        std::atomic<bool> _isBootstrapped;

        void ReserveBlock(const ::Memory::IAllocator* allocator, BigInt value);

    public:
        IdentityManager();

        void Bootstrap(
            const Schemas::IdentitySchema* schema,
            Int tableId,
            Int columnId
        );

        template<DataTypes::IsInteger T>
        [[nodiscard]] T Generate(const ::Memory::IAllocator* allocator);

        template<DataTypes::IsInteger T>
        [[nodiscard]] T ReserveRange(const ::Memory::IAllocator* allocator, T range);

        template<DataTypes::IsInteger T>
        [[nodiscard]] bool TryGenerate(const ::Memory::IAllocator* allocator, T& value);
        void UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] BigInt GetIncrement() const;
    };
}
