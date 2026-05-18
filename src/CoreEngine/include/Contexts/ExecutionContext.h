#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../Memory/Allocator.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"

namespace DataTypes{
    class String;
}

namespace Memory{
    class IAllocator;
}

class Variable;

namespace CoreEngine {
    namespace StorageTypes{
        class Table;
    }

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

        Snapshot(const Snapshot& other) = default;
        Snapshot& operator=(const Snapshot& other) = default;

        Snapshot(Snapshot&& other) noexcept{
            this->transactionId = other.transactionId;
            this->minimumTransactionId = other.minimumTransactionId;
            this->maximumTransactionId = other.maximumTransactionId;
            this->activeTransactionIds = std::move(other.activeTransactionIds);
        }

        Snapshot& operator=(Snapshot&& other) noexcept{
            if (this == &other)
                return *this;

            this->transactionId = other.transactionId;
            this->minimumTransactionId = other.minimumTransactionId;
            this->maximumTransactionId = other.maximumTransactionId;
            this->activeTransactionIds = std::move(other.activeTransactionIds);

            return *this;
        }

        [[nodiscard]] bool IsSystemTransaction()const{ return this->transactionId == FIRST_TRANSACTION_ID; }
    };

    struct ExecutionSchema{
        const StorageTypes::Table* tables[Constants::MAX_QUERY_JOINS];
        UnsignedInt tableCount;

        ExecutionSchema(): tables{nullptr}, tableCount(0){}
    };

    class ExecutionContext {
        ExecutionSchema schema;

        Snapshot snapshot;
        Memory::Allocator allocator;
        const Dictionary<DataTypes::String, Variable>* variables;
        Int batchSize;

        constexpr static UnsignedInt DEFAULT_ALLOCATION_SIZE = 1024 * 1024 * 10;

        public:
            ExecutionContext(
                const Snapshot &snapshot,
                Int batchSize,
                const Dictionary<DataTypes::String, Variable>& variables,
                Int initialAllocatorSize = DEFAULT_ALLOCATION_SIZE
            );
            ExecutionContext();
            ExecutionContext(ExecutionContext&& other) noexcept;
            ExecutionContext& operator=(ExecutionContext&& other) noexcept;

            ~ExecutionContext();

            void SetBatchSize(Int size);

            [[nodiscard]] const ::Memory::IAllocator* GetAllocator()const;
            const Dictionary<DataTypes::String, Variable>* GetVariables()const;
            [[nodiscard]] Int GetBatchSize()const;
            [[nodiscard]] transaction_id_t GetCurrentTransactionId()const;
            [[nodiscard]] const Snapshot& GetSnapshot()const;

            void AddTable(const StorageTypes::Table* table);
            const StorageTypes::Table* GetTable(UnsignedInt index) const;

            void ResetAllocator()const;
            bool IsAllocatorEmpty()const;

            void* Allocate(Int size) const;
            template<typename T>
            T* Allocate() const{
                return new (this->Allocate(sizeof(T))) T;
            }
            template<typename T, typename... Args>
            T* Allocate(Args&&... args) const{
                return new (this->Allocate(sizeof(T))) T(std::forward<Args>(args)...);
            }

            static ExecutionContext BaseContext();
    };
}
