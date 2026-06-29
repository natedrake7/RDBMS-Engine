#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/Mutex.h"

namespace CoreEngine{
    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager {
        Headers::IdentityColumnsHeader header;
        mutable MultiThreading::Mutex mutex;

        std::atomic<BigInt> reservedUpTo;
        std::atomic<BigInt> counter;

        void ReserveBlock(const ::Memory::IAllocator* allocator, BigInt value);

    public:
        IdentityManager();

        void SetHeaderIds(Int tableId, Int columnId);
        void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
        [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

        [[nodiscard]] BigInt Generate(const ::Memory::IAllocator* allocator);
        [[nodiscard]] bool TryGenerate(const ::Memory::IAllocator* allocator, BigInt& value);
        void UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const;

        [[nodiscard]] bool IsValid()const;
    };
}
