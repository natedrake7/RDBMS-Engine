#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"

#include <mutex>

namespace DatabaseEngine
{
    class ExecutionContext;
}

namespace DatabaseEngine::StorageTypes{
    class IdentityManager {
        Headers::IdentityColumnsHeader header;
        BigInt startingValue;

        mutable MultiThreading::ReadWriteMutex mutex;

        void UpdateMasterDb(const Memory::Allocator& allocator, BigInt value)const;

    public:
        IdentityManager();
        ~IdentityManager();

        void SetHeaderIds(Int tableId, Int columnId);
        void SetHeader(const Headers::IdentityColumnsHeader& newHeader);
        [[nodiscard]] const Headers::IdentityColumnsHeader& GetHeader() const;

        [[nodiscard]] BigInt Generate(const Memory::Allocator& allocator);
        [[nodiscard]] bool TryGenerate(const Memory::Allocator& allocator, BigInt& value);
        void UpdateMasterDb(const Memory::Allocator& allocator)const;

        [[nodiscard]] bool IsValid()const;
    };
}
