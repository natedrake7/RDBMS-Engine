#include "../../../include/Contexts/ExecutionContext.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../include/Vectorization/Vectorization.h"
#include "../../../include/DataStorage/Table.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/Database.h"

namespace CoreEngine::StorageTypes{
    template <typename T>
    void Table::MaterializeColumnFromPage(
        const ExecutionContext& context,
        const SelectionVector* sv,
        object_t* __restrict__ _data,
        const column_index_t columnIndex
    ) const{
        const auto& scanHandle = context.GetScanHandle(0);

        for (auto i = 0; i < sv->selectedRidsCount; i++){
            const auto rid = scanHandle.rids[sv->selectedRids[0][i]];

            const auto page = Storage::StorageManager::Get().GetPage<Pages::IndexPageView>(
                this->database->DataFileKey(),
                rid._pageId
            );

            std::memcpy(_data + (i * sizeof(T)), page.GetColumnAt(rid._index, columnIndex), sizeof(T));
        }
    }

    template <typename T>
    void Table::MaterializeColumnFromPage(
        const ExecutionContext& context,
        const Int rangeEnd,
        object_t* __restrict__ _data,
        const column_index_t columnIndex
    ) const{
        if (rangeEnd == 0)
            return;

        auto* __restrict__ output = reinterpret_cast<T*>(_data);
        const auto& scanHandle = context.GetScanHandle(0);

        for (auto i = 0; i < rangeEnd; i++){
            const auto rid = scanHandle.rids[i];

            const auto page = Storage::StorageManager::Get().GetPage<Pages::IndexPageView>(
                this->database->DataFileKey(),
                rid._pageId
            );

            std::memcpy(&output[i], page.GetColumnAt(rid._index, columnIndex), sizeof(T));
        }
    }

    template void Table::MaterializeColumnFromPage<bool> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<TinyInt> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<SmallInt> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<Int> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<BigInt> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<DataTypes::DateTime> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<DataTypes::Guid> (const ExecutionContext&, const SelectionVector*, object_t* __restrict__, column_index_t) const;

    template void Table::MaterializeColumnFromPage<bool> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<TinyInt>(const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<SmallInt> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<Int> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<BigInt> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<DataTypes::DateTime> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumnFromPage<DataTypes::Guid> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
}
