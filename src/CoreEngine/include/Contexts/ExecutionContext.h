#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../Memory/Allocator.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../BufferPool/FileKey.h"
#include "../DataStorage/Row.h"

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
            return snapshot.activeTransactionIds.Contains(transactionId);
        }
    };

    struct ExecutionSchema{
        const StorageTypes::Table* tables[Constants::MAX_QUERY_JOINS];
        Storage::FileKey fileKeys[Constants::MAX_QUERY_JOINS][StorageTypes::RID::Source::Count];
        UnsignedInt tableCount;

        ExecutionSchema()
            : tables{nullptr}, tableCount(0){}
    };

    struct ScanHandle{
        const StorageTypes::RID* rids;
        UnsignedInt size;

        ScanHandle()
            : rids(nullptr), size(0){}
        ScanHandle(const StorageTypes::RID* rids, const UnsignedInt size)
            : rids(rids), size(size){}
    };

    class ExecutionContext {
        ScanHandle scanHandles[Constants::MAX_QUERY_JOINS];
        ExecutionSchema schema;

        Snapshot snapshot;
        Memory::Allocator allocator;

        const Dictionary<DataTypes::String, Variable>* variables;

        Int batchSize;

        constexpr static UnsignedInt DEFAULT_ALLOCATION_SIZE = 1024 * 1024 * 10;

        public:
            ExecutionContext(
                Snapshot& snapshot,
                Int batchSize,
                const Dictionary<DataTypes::String, Variable>* variables,
                Int initialAllocatorSize = DEFAULT_ALLOCATION_SIZE
            );
            ExecutionContext();
            ExecutionContext(ExecutionContext&& other) noexcept;
            ExecutionContext& operator=(ExecutionContext&& other) noexcept;

            ~ExecutionContext();

            void SetBatchSize(Int size);

            [[nodiscard]] const ::Memory::IAllocator* GetAllocator()const;
            const Dictionary<DataTypes::String, Variable>* GetVariables()const;
            const Variable* GetVariable(const DataTypes::String& name) const;

            [[nodiscard]] Int GetBatchSize()const;
            [[nodiscard]] transaction_id_t GetCurrentTransactionId()const;
            [[nodiscard]] const Snapshot& GetSnapshot()const;

            void SetTable(const StorageTypes::Table* table, UnsignedSmallInt slotIndex);
            const StorageTypes::Table* GetTable(UnsignedSmallInt index) const;
            const Storage::FileKey* GetFileKeys(UnsignedSmallInt index) const;
            Storage::FileKey GetFileKey(UnsignedSmallInt slotIndex, UnsignedSmallInt index) const;

            void SetFileKey(
                Storage::FileKey fileKey,
                StorageTypes::RID::Source storageType,
                UnsignedSmallInt slotIndex
            );

            void SetScanHandle(const StorageTypes::RID* rids, UnsignedInt size, UnsignedSmallInt slotIndex);
            const ScanHandle& GetScanHandle(UnsignedInt index) const;
            [[nodiscard]] UnsignedInt GetScanHandleSize(UnsignedInt index) const;

            [[nodiscard]] const StorageTypes::RID* GetRIDPtr(UnsignedInt slotIndex, UnsignedInt ridIndex) const;
            [[nodiscard]] StorageTypes::RID GetRID(UnsignedInt slotIndex, UnsignedInt ridIndex) const;

            void ResetAllocator()const;
            void ReleaseAllocator()const;
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
