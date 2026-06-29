#include <cmath>
#include "../../../include/DataStorage/Table.h"
#include "DataStorage/Column.h"
#include "DataStorage/InsertPayload.h"
#include "Evaluators/Expression.h"

namespace CoreEngine::StorageTypes{
    template <typename ValueProvider>
    InsertPayload Table::SerializeRow(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const RowHeader& rowHeader,
        ValueProvider&& provider
    ) const{
        Int dataEntriesOffset = sizeof(RowHeader);

        // Only allocate offset space for non-NULL columns
        const auto dataOffSet = dataEntriesOffset + this->_columns.Size() * sizeof(RowEntry);
        auto payload = InsertPayload(allocator, this->payloadSize + dataOffSet, dataOffSet);

        std::memcpy(payload.Data(), &rowHeader, sizeof(RowHeader));

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

            const auto value = provider(ordinalPosition, autoComputedColumns);

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

            if (value.Size() > column->Size()){
                status.code = Errors::RuntimeError::ColumnSizeExceeded;
                return payload;
            }

            const auto size = value.Size();

            const auto offSet = payload.SetData(value.Data(), value.Size());
            // Write offset for all columns
            RowEntry rowEntry(offSet, RowEntry::INLINE, size);
            payload.SetData(&rowEntry, sizeof(RowEntry), dataEntriesOffset);
            dataEntriesOffset += sizeof(RowEntry);
        }

        payload.AlignSizeWithOffset();
        return payload;
    }

    InsertPayload Table::CreateInsertPayload(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const RowHeader& rowHeader,
        const DataStructures::PolymorphicArray<Value>& values
    ) const{
        return this->SerializeRow(status, allocator, rowHeader,
        [&](const Int ordinalPosition, const Int autoComputedColumns) -> const Value&{
            return values[ordinalPosition - autoComputedColumns];
        });
    }

    InsertPayload Table::CreateInsertPayload(
        Errors::RuntimeStatus& status,
        const ::Memory::IAllocator* allocator,
        const RowHeader& rowHeader,
        const DataStructures::PolymorphicArray<Expressions::Expression*>& expressions,
        const InsertPlan& insertPlan,
        const Expressions::EvaluationContext& evaluationContext
    ) const{
        Value value;
        return this->SerializeRow(status, allocator, rowHeader,
            [&](const Int ordinalPosition, const Int _) -> const Value&{
                const auto& slot = insertPlan._slotMap[ordinalPosition];
                switch (slot._kind)
                {
                    case InsertSlot::SlotKind::Value:
                        value = Expressions::EvaluateExpression(expressions[slot._slot], evaluationContext);
                        break;
                    case InsertSlot::SlotKind::Default:
                        value = Expressions::EvaluateExpression(insertPlan._sharedDefaults[slot._slot], evaluationContext);
                        break;
                    case InsertSlot::SlotKind::Null:
                        value = Value::Null();
                        break;
                }
                return value;
            }
        );
    }
}
