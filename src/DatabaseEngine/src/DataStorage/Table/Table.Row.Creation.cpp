#include <cmath>
#include "../../../include/DataStorage/Table.h"
#include "DataStorage/InsertPayload.h"


namespace DatabaseEngine::StorageTypes{
    InsertPayload Table::CreateInsertPayload(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const transaction_id_t transactionId,
        const std::vector<Value> &inputData
    ) const{
        auto rowHeader = RowHeader(allocator, static_cast<Int>(this->_columns.size()));
        rowHeader.version.createdTransactionId = transactionId;

        // First pass: determine how many non-NULL columns we have
        UnsignedSmallInt nonNullColumnCount = 0;
        UnsignedSmallInt dataSize = 0;

        UnsignedInt intermediateComputedColumns = 0;
        for (const auto& column : this->_columns){
            dataSize += column->Size();

            const auto columnOrdinal = column->OrdinalPosition();
            const auto index = columnOrdinal - intermediateComputedColumns;

            if (column->HasIdentity()){
                nonNullColumnCount++;
                intermediateComputedColumns++;
                continue;
            }

            auto& value = inputData.at(index);
            if (value.IsNull()){
                rowHeader.nullBitMap.Set(columnOrdinal, true);
                continue;
            }

            if (value.Size() >= LARGE_OBJECT_THRESHOLD_SIZE)
                rowHeader.largeObjectBitMap.Set(columnOrdinal, true);

            nonNullColumnCount++;
        }

        //calculate header size and offset
        // Header: version header + 3 bitmaps
        const auto bitmapBytes = static_cast<Int>(std::ceil(static_cast<double>(this->_columns.size()) / 8.0));
        const auto bitmapSize = 3 * bitmapBytes;
        Int dataSizesOffset = Constants::ROW_VERSION_HEADER_SIZE + bitmapSize;

        // Only allocate offset space for non-NULL columns
        const auto dataOffSet = dataSizesOffset + nonNullColumnCount * sizeof(block_size_t);

        auto payload = InsertPayload(allocator, dataSize + dataOffSet, dataOffSet);
        intermediateComputedColumns = 0;
        for (const auto& column : this->_columns){
            //ignore auto-computed columns even if specified
            if (column->HasIdentity()){
                const auto columnSize = column->Size();
                auto identityValue = column->GenerateIdentityValue(allocator);
                payload.SetData(&identityValue, columnSize);

                payload.SetData(&columnSize, sizeof(block_size_t), dataSizesOffset);
                dataSizesOffset += sizeof(block_size_t);

                intermediateComputedColumns++;
                continue;
            }

            const auto& index = column->OrdinalPosition() - intermediateComputedColumns;
            const auto& value = inputData.at(index);

            // Skip NULL values (already marked in bitmap)
            if (value.IsNull())
                continue;

            if (rowHeader.largeObjectBitMap.Get(index)){
                block_size_t size = value.Size();
                page_offset_t offSet = 0;
                const auto pageId = this->StoreLargeObject(
                    value,
                    offSet,
                    size,
                    nullptr
                );
                payload.SetData(&pageId, sizeof(page_id_t));

                constexpr auto pageIdSize = sizeof(page_id_t);
                payload.SetData(&pageIdSize, sizeof(block_size_t), dataSizesOffset);
                dataSizesOffset += sizeof(block_size_t);
                continue;
            }

            auto result = static_cast<block_size_t>(payload.SetData(value, column, status));
            if (!status.IsOk())
                return payload;

            // Write offset only for non-NULL columns
            payload.SetData(&result, sizeof(block_size_t), dataSizesOffset);
            dataSizesOffset += sizeof(block_size_t);
        }

        payload.AlignSizeWithOffset();

        auto headerOffset = 0;
        payload.SetData(&rowHeader.version.createdTransactionId, sizeof(transaction_id_t), headerOffset);
        headerOffset += sizeof(transaction_id_t);
        payload.SetData(&rowHeader.version.deletedTransactionId, sizeof(transaction_id_t), headerOffset);
        headerOffset += sizeof(transaction_id_t);
        payload.SetData(&rowHeader.version.olderVersionPointer.pageId, sizeof(page_id_t), headerOffset);
        headerOffset += sizeof(page_id_t);
        payload.SetData(&rowHeader.version.olderVersionPointer.offset, sizeof(page_offset_t), headerOffset);
        headerOffset += sizeof(page_offset_t);

        // Set bitmaps
        payload.SetData(rowHeader.nullBitMap.DataPtr(), bitmapBytes, headerOffset);
        headerOffset += bitmapBytes;
        payload.SetData(rowHeader.largeObjectBitMap.DataPtr(), bitmapBytes, headerOffset);
        headerOffset += bitmapBytes;
        payload.SetData(rowHeader.overflowBitMap.DataPtr(), bitmapBytes, headerOffset);
        // headerOffset += bitmapBytes;

        return payload;
    }

    InsertPayload Table::CreateUpdatePayload(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const transaction_id_t transactionId,
        const std::vector<Value>& inputData
    ) const{
        auto rowHeader = RowHeader(allocator, static_cast<Int>(this->_columns.size()));
        rowHeader.version.createdTransactionId = transactionId;

        // First pass: determine how many non-NULL columns we have
        UnsignedSmallInt nonNullColumnCount = 0;
        UnsignedSmallInt dataSize = 0;

        for (const auto& column : this->_columns){
            dataSize += column->Size();

            const auto columnOrdinal = column->OrdinalPosition();

            auto& value = inputData.at(columnOrdinal);
            if (value.IsNull()){
                rowHeader.nullBitMap.Set(columnOrdinal, true);
                continue;
            }

            if (value.Size() >= LARGE_OBJECT_THRESHOLD_SIZE)
                rowHeader.largeObjectBitMap.Set(columnOrdinal, true);

            nonNullColumnCount++;
        }

        //calculate header size and offset
        // Header: version header + 3 bitmaps
        const auto bitmapBytes = static_cast<Int>(std::ceil(static_cast<double>(this->_columns.size()) / 8.0));
        const auto bitmapSize = 3 * bitmapBytes;
        Int dataSizesOffset = Constants::ROW_VERSION_HEADER_SIZE + bitmapSize;

        // Only allocate offset space for non-NULL columns
        const auto dataOffSet = dataSizesOffset + nonNullColumnCount * sizeof(block_size_t);

        auto payload = InsertPayload(allocator, dataSize + dataOffSet, dataOffSet);
        for (const auto& column : this->_columns){
            const auto& index = column->OrdinalPosition();
            const auto& value = inputData.at(index);

            // Skip NULL values (already marked in bitmap)
            if (value.IsNull())
                continue;

            if (rowHeader.largeObjectBitMap.Get(index)){
                block_size_t size = value.Size();
                page_offset_t offSet = 0;
                const auto pageId = this->StoreLargeObject(
                    value,
                    offSet,
                    size,
                    nullptr
                );
                payload.SetData(&pageId, sizeof(page_id_t));

                constexpr auto pageIdSize = sizeof(page_id_t);
                payload.SetData(&pageIdSize, sizeof(block_size_t), dataSizesOffset);
                dataSizesOffset += sizeof(block_size_t);
                continue;
            }

            auto result = static_cast<block_size_t>(payload.SetData(value, column, status));
            if (!status.IsOk())
                return payload;

            // Write offset only for non-NULL columns
            payload.SetData(&result, sizeof(block_size_t), dataSizesOffset);
            dataSizesOffset += sizeof(block_size_t);
        }

        payload.AlignSizeWithOffset();

        auto headerOffset = 0;
        payload.SetData(&rowHeader.version.createdTransactionId, sizeof(transaction_id_t), headerOffset);
        headerOffset += sizeof(transaction_id_t);
        payload.SetData(&rowHeader.version.deletedTransactionId, sizeof(transaction_id_t), headerOffset);
        headerOffset += sizeof(transaction_id_t);
        payload.SetData(&rowHeader.version.olderVersionPointer.pageId, sizeof(page_id_t), headerOffset);
        headerOffset += sizeof(page_id_t);
        payload.SetData(&rowHeader.version.olderVersionPointer.offset, sizeof(page_offset_t), headerOffset);
        headerOffset += sizeof(page_offset_t);

        // Set bitmaps
        payload.SetData(rowHeader.nullBitMap.DataPtr(), bitmapBytes, headerOffset);
        headerOffset += bitmapBytes;
        payload.SetData(rowHeader.largeObjectBitMap.DataPtr(), bitmapBytes, headerOffset);
        headerOffset += bitmapBytes;
        payload.SetData(rowHeader.overflowBitMap.DataPtr(), bitmapBytes, headerOffset);
        // headerOffset += bitmapBytes;

        return payload;
    }
}
