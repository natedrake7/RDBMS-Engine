#include "../../../include/Contexts/ExecutionContext.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataTypes/StringValue.h"
#include "../../../include/Vectorization/Vectorization.h"
#include "../../../include/DataStorage/Table.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/Database.h"

namespace CoreEngine::StorageTypes{
    template <typename T>
    void Table::MaterializeColumn(
        const Table* table,
        const ExecutionContext& context,
        const SelectionVector* sv,
        DataVector* __restrict__ _vector,
        const UnsignedSmallInt slotIndex,
        const column_index_t ordinalPosition
    ){
        table->MaterializeColumn<T>(context, sv, _vector, slotIndex, ordinalPosition);
    }

    template <typename T>
    void Table::MaterializeColumn(
        const ExecutionContext& context,
        const SelectionVector* sv,
        DataVector* __restrict__ _vector,
        const UnsignedSmallInt slotIndex,
        const column_index_t ordinalPosition
    ) const{

        static auto& storageManager = Storage::StorageManager::Get();
        const auto& scanHandle = context.GetScanHandle(slotIndex);
        const auto* allocator = context.GetAllocator();
        const auto isIdentity = sv->isIdentity;

        auto* __restrict__ output = reinterpret_cast<T*>(_vector->_data);

        page_id_t cachedPageId = INVALID_PAGE_ID;
        Pages::PageView cachedPage;

        auto lazyFetchPage = [&](const page_id_t pageId){
            if (pageId != cachedPageId){
                cachedPage = storageManager.GetPage<Pages::PageView>(
                    this->_db->DataFileKey(),
                    pageId
                );
                cachedPageId = pageId;
            }
        };

        bool outNull = false;
        if (isIdentity){
            for (Int index = 0;index < sv->selectedRidsCount; index++){
                const auto rid = scanHandle.rids[index];
                lazyFetchPage(rid._pageId);
                output[index] = cachedPage.GetColumnAt<T>(allocator, rid._index, ordinalPosition, &outNull);
                _vector->SetNullValue(index, outNull);
            }

            return;
        }

        for (auto index = 0; index < sv->selectedRidsCount; index++){
            const auto rid = scanHandle.rids[sv->selectedRids[slotIndex][index]];
            lazyFetchPage(rid._pageId);
            output[index] = cachedPage.GetColumnAt<T>(allocator, rid._index, ordinalPosition, &outNull);
            _vector->SetNullValue(index, outNull);
        }
    }

    template <typename T>
    void Table::MaterializeColumn(
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
                this->_db->DataFileKey(),
                rid._pageId
            );

            std::memcpy(&output[i], page.GetColumnAt(rid._index, columnIndex), sizeof(T));
        }
    }

    template void Table::MaterializeColumn<bool> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<TinyInt> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<SmallInt> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<Int> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<BigInt> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::DateTime> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::Guid> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::StringValue> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::Decimal> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::JsonBinary> (const Table*, const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);

    template void Table::MaterializeColumn<bool> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<TinyInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<SmallInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<Int> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<BigInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::DateTime> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::Guid> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::StringValue> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::Decimal> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::JsonBinary> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t) const;


    template void Table::MaterializeColumn<bool> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<TinyInt>(const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<SmallInt> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<Int> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<BigInt> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::DateTime> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
    template void Table::MaterializeColumn<DataTypes::Guid> (const ExecutionContext&, Int, object_t* __restrict__, column_index_t) const;
}
