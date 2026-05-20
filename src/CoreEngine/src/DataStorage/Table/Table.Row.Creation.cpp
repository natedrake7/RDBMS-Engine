#include <cmath>
#include "../../../include/DataStorage/Table.h"
#include "DataStorage/InsertPayload.h"

namespace CoreEngine::StorageTypes{
    InsertPayload Table::CreateInsertPayload(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const transaction_id_t transactionId,
        const Int dataSize,
        const DataStructures::PolymorphicArray<Value> &inputData
    ) const{
        RowHeader rowHeader;
        rowHeader._createdTransactionId = transactionId;

        const auto columnsSize = this->_columns.Size();
        auto dataEntriesOffset = Constants::ROW_VERSION_HEADER_SIZE;

        // Only allocate offset space for non-NULL columns
        const auto dataOffSet = dataEntriesOffset + columnsSize * sizeof(RowEntry);
        auto payload = InsertPayload(allocator, dataSize + dataOffSet, dataOffSet);

        std::memcpy(payload.Data(), &rowHeader, Constants::ROW_VERSION_HEADER_SIZE);

        auto autoComputedColumns = 0;
        for (const auto& column : this->_columns){
            const auto ordinalPosition = column->OrdinalPosition();

            //ignore auto-computed columns even if specified
            if (column->HasIdentity()){
                const auto columnSize = column->Size();
                auto identityValue = column->GenerateIdentityValue(allocator);
                const auto dataOffset = payload.SetData(&identityValue, columnSize);

                RowEntry rowEntry(dataOffset, RowEntry::INLINE, columnSize);
                payload.SetData(&rowEntry, sizeof(RowEntry), dataEntriesOffset);
                dataEntriesOffset += sizeof(RowEntry);
                autoComputedColumns++;
                continue;
            }

            const auto& value = inputData[ordinalPosition - autoComputedColumns];
            if (value.IsNull()){
                RowEntry rowEntry(0, RowEntry::NULLVAL, 0);
                payload.SetData(&rowEntry, sizeof(RowEntry), dataEntriesOffset);
                dataEntriesOffset += sizeof(RowEntry);
                continue;
            }

            if (value.Size() >= Constants::LARGE_OBJECT_THRESHOLD_SIZE){
                block_size_t size = value.Size();
                page_offset_t offSet = 0;
                const auto pageId = this->StoreLargeObject(
                    allocator,
                    value,
                    offSet,
                    size,
                    nullptr
                );
                const auto dataOffset = payload.SetData(&pageId, sizeof(page_id_t));
                RowEntry rowEntry(dataOffset, RowEntry::LOB, sizeof(page_id_t));
                payload.SetData(&rowEntry, sizeof(RowEntry), dataEntriesOffset);
                dataEntriesOffset += sizeof(RowEntry);
                continue;
            }

            const auto offSet = payload.Offset();
            const auto result = static_cast<block_size_t>(payload.SetData(value, column, status));
            if (!status.IsOk()) return payload;

            // Write offset for all columns
            RowEntry rowEntry(offSet, RowEntry::INLINE, result);
            payload.SetData(&rowEntry, sizeof(RowEntry), dataEntriesOffset);
            dataEntriesOffset += sizeof(RowEntry);
        }

        payload.AlignSizeWithOffset();
        return payload;
    }
}
