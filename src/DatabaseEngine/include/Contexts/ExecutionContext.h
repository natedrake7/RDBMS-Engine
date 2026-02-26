#pragma once
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/Memory/Allocator.h"
#include "../Managers/GlobalMemoryManager.h"

class Variable;

namespace DatabaseEngine {
    struct ScanState;

    struct Snapshot {
        transaction_id_t transactionId;

        transaction_id_t minimumTransactionId;
        transaction_id_t maximumTransactionId;
        HashSet<transaction_id_t> activeTransactionIds;

        Snapshot() {
            this->transactionId = FIRST_TRANSACTION_ID;
            this->minimumTransactionId = FIRST_TRANSACTION_ID;
            this->maximumTransactionId = FIRST_TRANSACTION_ID;
        }

        [[nodiscard]] bool IsSystemTransaction()const{ return this->transactionId == FIRST_TRANSACTION_ID; }
    };

    class ExecutionContext {
        Snapshot snapshot;
        Memory::Allocator allocator;
        const Dictionary<std::string, Variable>* variables;
        Int batchSize;

        constexpr static UnsignedInt DEFAULT_ALLOCATION_SIZE = 1024 * 1024 * 10;

        public:
            ExecutionContext(
                const Snapshot &snapshot,
                Int batchSize,
                const Dictionary<std::string, Variable>& variables,
                Int initialAllocatorSize = DEFAULT_ALLOCATION_SIZE
            );
            ExecutionContext();
            ExecutionContext(ExecutionContext&& other) noexcept;
            ExecutionContext& operator=(ExecutionContext&& other) noexcept;

            ~ExecutionContext();

            void SetBatchSize(Int size);

            [[nodiscard]] const Memory::Allocator& GetAllocator()const;
            const Dictionary<std::string, Variable>* GetVariables()const;
            [[nodiscard]] Int GetBatchSize()const;
            [[nodiscard]] transaction_id_t GetCurrentTransactionId()const;
            [[nodiscard]] const Snapshot& GetSnapshot()const;

            void* Allocate(Int size) const;
            template<typename T>
            T* Allocate() const{
                return new (this->Allocate(sizeof(T))) T;
            }
            template<typename T, typename... Args>
            T* Allocate(Args&&... args) const{
                return new (this->Allocate(sizeof(T))) T(std::forward<Args>(args)...);
            }
    };
}
