#include <cmath>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/DataStorage/Row/SerializedRow.h>
#include <CoreEngine/DataStorage/LargeObjects/LobWriter.h>
#include <CoreEngine/DataStorage/Row/Row.SerializationContext.h>
#include <CoreEngine/Evaluators/Expression.h>
#include <CoreEngine/Messages.h>
#include <CoreEngine/DataStorage/Row/Row.InsertPlan.h>

namespace CoreEngine::StorageTypes{
    template <typename ValueProvider>
    void Table::EvaluateRow(RowSerializationContext& context, ValueProvider&& provider){
        Int autoComputed = 0;

        auto* __restrict__ values = context._values;

        for (auto& column: this->Columns()){
            if (column._identity != nullptr){
                values[column._ordinalPosition] = this->GenerateIdentityValue(context._allocator, column._ordinalPosition);
                autoComputed++;
                continue;
            }

            values[column._ordinalPosition] = provider(column._ordinalPosition, autoComputed);
        }
    }

    Errors::RuntimeStatus Table::PlanRow(const RowSerializationContext& context, UnsignedInt& outRowSize) const{
        UnsignedInt rowSize = sizeof(RowHeader) + this->ColumnCount() * sizeof(RowEntry);

        for (const auto& column : this->Columns()){
            const auto* __restrict__ value = &context._values[column._ordinalPosition];
            auto* __restrict__ placement = &context._placements[column._ordinalPosition];

            if (value->IsNull()){
                placement->_kind = ColumnPlacement::Kind::Null;
                placement->_size = 0;
                continue;
            }

            if (value->IsLob()){
                placement->_kind = ColumnPlacement::Kind::ExistingLob;
                placement->_size = LOB_REFERENCE_SIZE;
                rowSize += LOB_REFERENCE_SIZE;
                continue;
            }

            if (value->Size() > column._recordSize){
                return Errors::RuntimeStatus(
                    Errors::RuntimeError::ColumnSizeExceeded,
                    Messages::COLUMN_SIZE_EXCEEDED(context._allocator, column._name)
                );
            }

            const auto valueSize = value->Size();

            placement->_kind = ColumnPlacement::Kind::Inline;
            placement->_size = valueSize;
            rowSize += valueSize;
        }

        const auto limit = this->MaxInlineRowSize();
        while (rowSize > limit){
            Int largest = -1;
            UnsignedInt largestSize = LOB_REFERENCE_SIZE;

            for (const auto& column : this->Columns()){
                const auto* __restrict__ placement = &context._placements[column._ordinalPosition];

                if (
                    placement->_kind == ColumnPlacement::Kind::Inline
                    && placement->_size > largestSize
                    && this->CanStoreColumnOffRow(column._ordinalPosition)
                ){
                    largest = column._ordinalPosition;
                    largestSize = placement->_size;
                }
            }

            if (largest == -1)
                return Errors::RuntimeStatus(
                    Errors::RuntimeError::ColumnSizeExceeded,
                    Messages::TABLE_LAYOUT_TOO_LARGE,
                    context._allocator
                );

            context._placements[largest]._kind = ColumnPlacement::Kind::OffRow;
            rowSize -= largestSize - LOB_REFERENCE_SIZE;
        }

        outRowSize = rowSize;
        return Errors::RuntimeStatus();
    }

    void Table::WriteOffRowValues(const RowSerializationContext& context) const{
        for (const auto& column : this->Columns()){
            auto* __restrict__ placement = &context._placements[column._ordinalPosition];

            if (placement->_kind != ColumnPlacement::Kind::OffRow)
                continue;

            auto* __restrict__ value = &context._values[column._ordinalPosition];
            const auto lobRef = LobWriter::Write(context._allocator, this, value->Data(), value->Size());
            *value = Value::FromLobReference(lobRef, value->GetType(), column._ordinalPosition);
        }
    }

    SerializedRow Table::WriteRow(const RowSerializationContext& context, const UnsignedInt rowSize) const{
        auto* __restrict__ buffer = static_cast<object_t*>(context._allocator->AllocateRaw(rowSize));

        const auto dataStart = sizeof(RowHeader) + this->ColumnCount() * sizeof(RowEntry);

        std::memcpy(buffer, &context._header, sizeof(RowHeader));
        auto payload = SerializedRow(buffer, rowSize, dataStart);

        Int entryOffset = sizeof(RowHeader);
        for (const auto& column : this->Columns()){
            const auto* __restrict__ placement = &context._placements[column._ordinalPosition];
            const auto* __restrict__ value = &context._values[column._ordinalPosition];

            RowEntry entry;
            switch (placement->_kind){
                case ColumnPlacement::Kind::Null:
                    entry = RowEntry(0, RowEntry::NULLVAL, 0);
                    break;
                case ColumnPlacement::Kind::Inline:{
                    const auto size = placement->_size;
                    const auto offSet = payload.SetData(value->Data(), size);
                    entry = RowEntry(offSet, RowEntry::INLINE, size);
                    break;
                }
                case ColumnPlacement::Kind::OffRow:
                case ColumnPlacement::Kind::ExistingLob:{
                    const auto lobReference = value->AsLobReference();
                    const auto offset = payload.SetData(&lobReference, LOB_REFERENCE_SIZE);
                    entry = RowEntry(offset, RowEntry::LOB, LOB_REFERENCE_SIZE);
                    break;
                }
            }
            payload.SetData(&entry, sizeof(RowEntry), entryOffset);
            entryOffset += sizeof(RowEntry);
        }

        payload.AlignSizeWithOffset();
        assert(payload.Size() == rowSize);
        return payload;
    }

    template <typename ValueProvider>
    SerializedRow Table::SerializeRowGeneric(
        Errors::RuntimeStatus& status,
        RowSerializationContext& rowContext,
        ValueProvider&& provider
    ){
        this->EvaluateRow(rowContext, std::forward<ValueProvider>(provider));
        UnsignedInt rowSize = 0;
        status = this->PlanRow(rowContext, rowSize);
        if (!status.IsOk())
            return SerializedRow();

        this->WriteOffRowValues(rowContext);
        return this->WriteRow(rowContext, rowSize);
    }

    SerializedRow Table::SerializeRow(
        Errors::RuntimeStatus& status,
        RowSerializationContext& rowContext,
        const DataStructures::PolymorphicArray<Value>& values
    ){
        return this->SerializeRowGeneric(status, rowContext,
        [&](const Int ordinalPosition, const Int autoComputedColumns) -> const Value&{
            return values[ordinalPosition - autoComputedColumns];
        });
    }
}
