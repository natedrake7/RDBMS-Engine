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
        struct RID;
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

    struct ScanHandle{
        const StorageTypes::RID* rids;
        UnsignedInt size;

        ScanHandle(): rids(nullptr), size(0){}
        ScanHandle(const StorageTypes::RID* rids, const UnsignedInt size): rids(rids), size(size){}
    };

    struct ScanContext{
        ScanHandle scanHandles[Constants::MAX_QUERY_JOINS];
        UnsignedInt scanHandleCount;

        ScanContext(): scanHandles{}, scanHandleCount(0) {}
    };

    struct SelectionVector{
        UnsignedInt* selectedRids[Constants::MAX_QUERY_JOINS];

        UnsignedTinyInt* nullMask[Constants::MAX_QUERY_JOINS];

        UnsignedTinyInt sourceMap[Constants::MAX_QUERY_JOINS];

        Int selectedRidsCount;
        bool isIdentity;

        SelectionVector()
            :   selectedRids{nullptr}, nullMask{nullptr},
                sourceMap{0}, selectedRidsCount(0), isIdentity(false) {}

        void AllocateRids(const ::Memory::IAllocator* allocator, Int index, Int size);
        void AllocateNullMask(const ::Memory::IAllocator* allocator, Int index, Int size);
    };

    class ExecutionContext {
        ScanContext scanContext;
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

            void AddScanHandle(const StorageTypes::RID* rids, UnsignedInt size);
            const ScanHandle& GetScanHandle(UnsignedInt index) const;

            [[nodiscard]] const StorageTypes::RID* GetRid(UnsignedInt scanHandleIndex, UnsignedInt ridIndex) const;

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
