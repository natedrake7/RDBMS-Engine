#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/Mutex.h"

namespace CoreEngine{
    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager {
        Headers::IdentityColumnsHeader header;
        BigInt startingValue;

        std::atomic<BigInt> counter;

        mutable MultiThreading::Mutex mutex;

        void UpdateMasterDb(const ::Memory::IAllocator* allocator, BigInt value);

    public:
        IdentityManager();
        ~IdentityManager();

        void SetHeaderIds(Int tableId, Int columnId);
        void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
        [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

        [[nodiscard]] BigInt Generate(const ::Memory::IAllocator* allocator);
        [[nodiscard]] bool TryGenerate(const ::Memory::IAllocator* allocator, BigInt& value);
        void UpdateMasterDb(const ::Memory::IAllocator* allocator)const;

        [[nodiscard]] bool IsValid()const;
    };
}
