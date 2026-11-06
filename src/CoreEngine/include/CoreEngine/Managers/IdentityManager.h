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


        std::atomic<Int64_t> _reservedUpTo;
        std::atomic<Int64_t> _counter;

        Int32_t _tableId;
        Int32_t _columnId;

        std::atomic<bool> _isBootstrapped;

        void ReserveBlock(const ::Memory::IAllocator* allocator, Int64_t value);

    public:
        IdentityManager();

        void Bootstrap(
            const Schemas::IdentitySchema* schema,
            Int32_t tableId,
            Int32_t columnId
        );

        template<DataTypes::IsInteger T>
        [[nodiscard]] T Generate(const ::Memory::IAllocator* allocator);

        template<DataTypes::IsInteger T>
        [[nodiscard]] T ReserveRange(const ::Memory::IAllocator* allocator, T range);

        template<DataTypes::IsInteger T>
        [[nodiscard]] bool TryGenerate(const ::Memory::IAllocator* allocator, T& value);
        void UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] Int64_t GetIncrement() const;

        void Reseed(Int64_t value);
    };
}
