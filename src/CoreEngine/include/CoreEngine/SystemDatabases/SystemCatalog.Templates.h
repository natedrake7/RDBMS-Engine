#pragma once
#include <cassert>
#include <meta>
#include <optional>
#include <utility>
#include <vector>

#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/MaterializedRow.h>
#include <CoreEngine/Database.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Indexing/Key.h>
#include <CoreEngine/Reflection/Rows.h>
#include <CoreEngine/SystemDatabases/CatalogRows.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>

namespace CoreEngine{
    namespace CatalogDetail{
        // The primary-key members of a row type, in key (declaration) order
        template <Catalog::CatalogRow TRow>
        [[nodiscard]] consteval auto KeyMembers(){
            std::vector<std::meta::info> keys;
            for (const auto member : Rows::Members<TRow>())
                if (::Reflection::HasAnnotation<Rows::PrimaryKey>(member))
                    keys.push_back(member);
            return std::define_static_array(keys);
        }

        // The clustered key of a row, encoded exactly like the hand-written Key(allocator, a, b) seeks
        template <Catalog::CatalogRow TRow>
        [[nodiscard]] DataTypes::Indexing::Key KeyOf(const TRow& row, const ::Memory::IAllocator* allocator){
            static constexpr auto keys = KeyMembers<TRow>();
            return [&]<std::size_t... I>(std::index_sequence<I...>){
                return DataTypes::Indexing::Key(allocator, row.[:keys[I]:]...);
            }(std::make_index_sequence<keys.size()>{});
        }

        // Every member except the primary key, as values tagged with their column ordinal
        template <Catalog::CatalogRow TRow>
        [[nodiscard]] DataStructures::PolymorphicArray<Value> NonKeyValues(const TRow& row, const ::Memory::IAllocator* allocator){
            DataStructures::PolymorphicArray<Value> values(allocator, Rows::ColumnCount<TRow>);
            column_index_t ordinal = 0;
            template for (constexpr auto member : Rows::Members<TRow>()){
                if constexpr (!::Reflection::HasAnnotation<Rows::PrimaryKey>(member))
                    values.Push(Rows::MakeValue(row.[:member:], allocator, ordinal));
                ++ordinal;
            }
            return values;
        }
    }

    template <Catalog::CatalogRow TRow>
    Errors::RuntimeStatus SystemCatalog::Insert(const ExecutionContext& context, const TRow& row) const{
        auto* table = this->masterDb->OpenTable(Catalog::TableOrdinalOf<TRow>);
        return table->SystemInsertRow(context, Rows::ToValues(row, context.GetAllocator()));
    }

    template <Catalog::CatalogRow TRow, DataTypes::Primitive... TKey> requires (sizeof...(TKey) > 0)
    DataStructures::PolymorphicArray<TRow> SystemCatalog::Select(
        const ::Memory::IAllocator* allocator,
        const TKey&... keyPrefix
    ) const{
        static_assert(sizeof...(TKey) <= Rows::KeyColumnsOf<TRow>().size(), "SystemCatalog::Select: more key values than key columns");

        auto* table = this->masterDb->OpenTable(Catalog::TableOrdinalOf<TRow>);

        DataStructures::PolymorphicArray<StorageTypes::RID> rids(allocator);
        const DataTypes::Indexing::Key key(allocator, keyPrefix...);
        table->SystemClusteredIndexSeek(allocator, &rids, key, nullptr);

        DataStructures::PolymorphicArray<TRow> rows(allocator, rids.Size());
        for (const auto& rid : rids){
            const auto materialized = table->MaterializeFromPage(allocator, &rid);
            rows.Push(Rows::FromRow<TRow>(materialized.Data(), allocator));
        }
        return rows;
    }

    template <Catalog::CatalogRow TRow, DataTypes::Primitive... TKey> requires (sizeof...(TKey) > 0)
    std::optional<TRow> SystemCatalog::SelectOne(
        const ::Memory::IAllocator* allocator,
        const TKey&... key
    ) const{
        static_assert(sizeof...(TKey) == Rows::KeyColumnsOf<TRow>().size(), "SystemCatalog::SelectOne: needs the full primary key");

        auto rows = this->Select<TRow>(allocator, key...);
        if (rows.Empty())
            return std::nullopt;

        assert(rows.Size() == 1 && "SystemCatalog::SelectOne: primary key matched more than one row");
        return std::move(rows[0]);
    }

    template <std::meta::info... Fields, Catalog::CatalogRow TRow>
    Errors::RuntimeStatus SystemCatalog::Update(const ::Memory::IAllocator* allocator, const TRow& row) const{
        static_assert(((std::meta::parent_of(Fields) == ^^TRow) && ...), "SystemCatalog::Update: a field is not a member of the row type");
        static_assert((!::Reflection::HasAnnotation<Rows::PrimaryKey>(Fields) && ...), "SystemCatalog::Update: primary key columns can't be updated");

        auto* table = this->masterDb->OpenTable(Catalog::TableOrdinalOf<TRow>);

        DataStructures::PolymorphicArray<Value> updates(allocator);
        if constexpr (sizeof...(Fields) == 0)
            updates = CatalogDetail::NonKeyValues(row, allocator);
        else{
            updates = DataStructures::PolymorphicArray<Value>(allocator, static_cast<Int>(sizeof...(Fields)));
            (updates.Push(Rows::MakeValue(row.[:Fields:], allocator, Rows::OrdinalOf(Fields))), ...);
        }

        return table->SystemClusteredIndexSeekUpdate(allocator, CatalogDetail::KeyOf(row, allocator), updates);
    }
}
