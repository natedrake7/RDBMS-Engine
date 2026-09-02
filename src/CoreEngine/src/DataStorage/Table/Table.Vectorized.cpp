#include "../../../include/Contexts/ExecutionContext.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataTypes/StringValue.h"
#include "../../../include/Vectorization/Vectorization.h"
#include "../../../include/DataStorage/Table.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/Database.h"

namespace CoreEngine::StorageTypes{
    template <typename T>
    void Table::MaterializeColumnFromPage(
        const Storage::FileKey* fileKeys,
        const ::Memory::IAllocator* allocator,
        const DataStructures::PolymorphicArray<RID>& rids,
        DataVector* __restrict__ _vector,
        const column_index_t ordinalPosition
    ){
        static auto& storageManager = Storage::StorageManager::Get();

        auto* __restrict__ output = reinterpret_cast<T*>(_vector->_data);
        const auto* __restrict__ ridsData = rids.Data();
        bool outNull = false;

        page_id_t cachedPageIds[RID::Source::Count] = {};
        Pages::PageView cachedPages[RID::Source::Count];

        const auto lazyFetchPage = [&](const page_id_t pageId, const RID::Source source){
            if (pageId != cachedPageIds[source]){
                cachedPages[source] = storageManager.GetPage<Pages::PageView>(
                    fileKeys[source],
                    pageId
                );
                cachedPageIds[source] = pageId;
            }
        };
        for (auto i = 0; i < rids.Size(); i++){
            const auto rid = ridsData[i];
            const auto source = ridsData[i].GetSource();

            lazyFetchPage(rid._pageId, source);
            output[i] = cachedPages[source].GetColumnAt<T>(allocator, rid._index, ordinalPosition, &outNull);
            _vector->SetNullValue(i, outNull);
        }
    }

    template <typename T>
    void Table::MaterializeColumn(
        const ExecutionContext& context,
        const SelectionVector* sv,
        DataVector* __restrict__ _vector,
        const UnsignedSmallInt slotIndex,
        const column_index_t ordinalPosition
    ){
        static auto& storageManager = Storage::StorageManager::Get();
        const auto isIdentity = sv->isIdentity;

        auto* __restrict__ output = reinterpret_cast<T*>(_vector->_data);

        const auto* allocator = context.GetAllocator();
        const auto* rids = context.GetRIDs(slotIndex);

        page_id_t cachedPageIds[RID::Source::Count] = {};
        Pages::PageView cachedPages[RID::Source::Count];

        const auto fileKeys = context.GetFileKeys(slotIndex);

        const auto lazyFetchPage = [&](const page_id_t pageId, const RID::Source source){
            if (pageId != cachedPageIds[source]){
                cachedPages[source] = storageManager.GetPage<Pages::PageView>(
                    fileKeys[source],
                    pageId
                );
                cachedPageIds[source] = pageId;
            }
        };

        bool outNull = false;
        if (isIdentity){
            for (Int index = 0;index < sv->selectedRidsCount; index++){
                const auto rid = rids[index];
                const auto source = rid.GetSource();

                lazyFetchPage(rid._pageId, source);
                output[index] = cachedPages[source].GetColumnAt<T>(allocator, rid._index, ordinalPosition, &outNull);
                _vector->SetNullValue(index, outNull);
            }

            return;
        }

        for (auto index = 0; index < sv->selectedRidsCount; index++){
            const auto rid = rids[sv->selectedRids[slotIndex][index]];
            const auto source = rid.GetSource();

            lazyFetchPage(rid._pageId, source);
            output[index] = cachedPages[source].GetColumnAt<T>(allocator, rid._index, ordinalPosition, &outNull);
            _vector->SetNullValue(index, outNull);
        }
    }

    template void Table::MaterializeColumnFromPage<bool>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<TinyInt>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<SmallInt>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<Int>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<BigInt>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<DataTypes::DateTime>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<DataTypes::Guid>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<DataTypes::StringValue>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<DataTypes::Decimal>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);
    template void Table::MaterializeColumnFromPage<DataTypes::JsonBinary>(const Storage::FileKey*, const ::Memory::IAllocator*, const DataStructures::PolymorphicArray<RID>&, DataVector* __restrict__, column_index_t);

    template void Table::MaterializeColumn<bool> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<TinyInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<SmallInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<Int> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<BigInt> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::DateTime> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::Guid> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::StringValue> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::Decimal> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
    template void Table::MaterializeColumn<DataTypes::JsonBinary> (const ExecutionContext&, const SelectionVector*, DataVector* __restrict__, UnsignedSmallInt, column_index_t);
}
