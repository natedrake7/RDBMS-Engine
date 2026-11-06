#pragma once
#include <Systemic/CancellationToken.h>
#include <CoreEngine/DatabaseConstants.h>
#include <Systemic/Constants.h>
#include <Systemic/DataStructures/HashSet.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Memory/Allocator.h>
#include <Systemic/DataStructures/Dictionary.h>
#include <CoreEngine/DataStorage/Row/Row.h>

#include "CoreEngine/Accessors/PageAccessor.h"

class BoundVariable;

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

        Snapshot()
            :   transactionId(FIRST_TRANSACTION_ID),
                minimumTransactionId(FIRST_TRANSACTION_ID), maximumTransactionId(FIRST_TRANSACTION_ID){}

        Snapshot(const Snapshot& other) = default;
        Snapshot& operator=(const Snapshot& other) = default;

        Snapshot(Snapshot&& other) noexcept
            :   transactionId(other.transactionId),
                minimumTransactionId(other.minimumTransactionId),
                maximumTransactionId(other.maximumTransactionId),
                activeTransactionIds(std::move(other.activeTransactionIds)){}

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
        static bool IsWriteVisible(const transaction_id_t transactionId, const Snapshot& snapshot){
            if (transactionId == snapshot.transactionId)
                return true;
            if (transactionId < snapshot.minimumTransactionId)
                return true;
            if (transactionId >= snapshot.maximumTransactionId)
                return false;
            if (snapshot.activeTransactionIds.Contains(transactionId))
                return false;
            return true;
        }
    };

    struct ExecutionSchema{
        const StorageTypes::Table* tables[Constants::MAX_QUERY_JOINS];
        UInt32_t tableCount;

        ExecutionSchema()
            : tables{nullptr}, tableCount(0){}
    };

    struct ScanHandle{
        const StorageTypes::RID* rids;
        UInt32_t size;

        ScanHandle()
            : rids(nullptr), size(0){}
        ScanHandle(const StorageTypes::RID* rids, const UInt32_t size)
            : rids(rids), size(size){}
    };

    class ExecutionContext {
        ScanHandle _scanHandles[Constants::MAX_QUERY_JOINS];
        ExecutionSchema _schema;

        Snapshot _snapshot;
        Memory::Allocator _allocator;

        CancellationToken _cancellationToken;

        const Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>>* _variables;

        Int32_t _batchSize;

        inline static constexpr UInt32_t DEFAULT_ALLOCATION_SIZE = 10 * Constants::MB;

        public:
            ExecutionContext(
                Snapshot& snapshot,
                Int32_t batchSize,
                const Dictionary<DataTypes::StringView, std::unique_ptr<BoundVariable>>* variables,
                Int32_t initialAllocatorSize = DEFAULT_ALLOCATION_SIZE
            );
            ExecutionContext();
            ExecutionContext(ExecutionContext&& other) noexcept;
            ExecutionContext& operator=(ExecutionContext&& other) noexcept;

            ~ExecutionContext();

            void SetBatchSize(Int32_t size);

            [[nodiscard]] const ::Memory::IAllocator* GetAllocator()const;
            const BoundVariable* GetVariable(const DataTypes::StringView& name) const;

            [[nodiscard]] Int32_t GetBatchSize()const;
            [[nodiscard]] transaction_id_t GetCurrentTransactionId()const;
            [[nodiscard]] const Snapshot& GetSnapshot()const;

            void AttachCancellationToken(CancellationToken& cancellationToken);

            void SetTable(const StorageTypes::Table* table, UInt16_t slotIndex);
            const StorageTypes::Table* GetTable(UInt16_t index) const;

            [[nodiscard]] Accessors::PageAccessor GetPageAccessor(UInt16_t slotIndex)const;

            void SetScanHandle(const StorageTypes::RID* rids, UInt32_t size, UInt16_t slotIndex);
            const ScanHandle& GetScanHandle(UInt32_t index) const;
            [[nodiscard]] UInt32_t GetScanHandleSize(UInt32_t index) const;

            [[nodiscard]] const StorageTypes::RID* GetRIDs(UInt32_t slotIndex)const;

            [[nodiscard]] const StorageTypes::RID* GetRIDPtr(UInt32_t slotIndex, UInt32_t ridIndex) const;
            [[nodiscard]] StorageTypes::RID GetRID(UInt32_t slotIndex, UInt32_t ridIndex) const;

            void ResetAllocator()const;
            void ReleaseAllocator()const;
            bool IsAllocatorEmpty()const;

            void* Allocate(Int32_t size) const;
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
