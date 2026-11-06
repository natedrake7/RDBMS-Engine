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
        const UInt16_t slotIndex,
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
            for (Int32_t index = 0;index < sv->selectedRidsCount; index++){
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

    template <typename T>
    void Table::DecodeVariableColumn(
        const ::Memory::IAllocator* allocator,
        const object_t* const* __restrict__ rows,
        const Int32_t count,
        DataVector* __restrict__ out,
        const UInt32_t start,
        const column_index_t ordinal
    ) const{
        const auto entryOffset = sizeof(RowHeader) + ordinal * sizeof(RowEntry);

        auto* __restrict__ values = out->DataAs<T>() + start;
        for (Int32_t i = 0;i < count; i++){
            const auto* row = rows[i];

            RowEntry entry;
            std::memcpy(&entry, row + entryOffset, sizeof(entry));

            const bool isNull = entry.IsNull();
            out->SetNullValue(start + i, isNull);
            if (isNull)
                continue;

            const auto* value = row + entry.Offset();
            if constexpr(DataTypes::Primitive<T>)
                std::memcpy(&values[i], value, sizeof(T));
            else{
                if (entry.Type() != RowEntry::INLINE) [[unlikely]]
                    throw std::logic_error("DecodeColumn: overflow and LOB columns are not supported yet");

                if constexpr (DataTypes::IsStringValue<T>)
                    values[i] = DataTypes::StringValue::Create(allocator, reinterpret_cast<const char*>(value), entry.Size());
                else if constexpr (DataTypes::IsDecimal<T>)
                    values[i] = DataTypes::Decimal(value, entry.Size());
                else if constexpr (DataTypes::IsJson<T>)
                    values[i] = DataTypes::JsonBinary(allocator, value, entry.Size());
                else
                    static_assert(DataTypes::AlwaysFalse<T>, "Unsupported type");
            }
        }

    }
}