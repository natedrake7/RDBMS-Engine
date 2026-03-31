#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"

namespace CoreEngine{
    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager {
        Headers::IdentityColumnsHeader header;
        BigInt startingValue;

        mutable MultiThreading::ReadWriteMutex mutex;

        void UpdateMasterDb(const ::Memory::IAllocator* allocator, BigInt value)const;

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
