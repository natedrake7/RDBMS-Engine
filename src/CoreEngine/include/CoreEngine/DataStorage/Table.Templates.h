#pragma once
#include <CoreEngine/Contexts/ExecutionContext.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringValue.h>
#include <CoreEngine/Vectorization/Vectorization.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include "CoreEngine/SystemDatabases/TemporaryDatabase.h"
#include "CoreEngine/SystemDatabases/VersionDatabase.h"

namespace CoreEngine::StorageTypes{
    template <typename T>
    void Table::MaterializeColumnFromPage(
        const Accessors::PageAccessor* pageAccessors,
        const ::Memory::IAllocator* allocator,
        const DataStructures::PolymorphicArray<RID>& rids,
        DataVector* __restrict__ _vector,
        const column_index_t ordinalPosition
    ){
        auto* __restrict__ output = reinterpret_cast<T*>(_vector->_data);
        const auto* __restrict__ ridsData = rids.Data();
        bool outNull = false;

        page_id_t cachedPageIds[RID::Source::Count]{
            INVALID_PAGE_ID, INVALID_PAGE_ID, INVALID_PAGE_ID
        };
        Pages::PageView cachedPages[RID::Source::Count];

        const auto lazyFetchPage = [&](const page_id_t pageId, const RID::Source source){
            if (pageId != cachedPageIds[source]){
                cachedPages[source] = pageAccessors[std::to_underlying(source)].GetPage<Pages::PageView>(pageId);
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
        const auto isIdentity = sv->isIdentity;

        auto* __restrict__ output = _vector->DataAs<T>();

        const auto* allocator = context.GetAllocator();
        const auto* rids = context.GetRIDs(slotIndex);

        page_id_t cachedPageIds[RID::Source::Count]{
            INVALID_PAGE_ID, INVALID_PAGE_ID, INVALID_PAGE_ID
        };

        Pages::PageView cachedPages[RID::Source::Count];

        const Accessors::PageAccessor accessors[RID::Source::Count]{
            context.GetPageAccessor(slotIndex),
            VersionDatabase::Get().GetPageAccessor(),// RID::Version
            TemporaryDatabase::Get().GetPageAccessor()// RID::TemporaryDb
        };

        const auto lazyFetchPage = [&](const page_id_t pageId, const RID::Source source){
            if (pageId != cachedPageIds[source]){
                cachedPages[source] = accessors[std::to_underlying(source)].GetPage<Pages::PageView>(pageId);
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
}