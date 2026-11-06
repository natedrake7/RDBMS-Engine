#pragma once
#include <utility>

#include <Systemic/Constants.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/DataStructures/StaticArray.h>
#include <Systemic/DataTypes/DateTime.h>
#include <Systemic/DataTypes/Decimal.h>
#include <Systemic/DataTypes/Value.h>
#include <CoreEngine/DataStorage/Schema/Schemas.h>

// Catalog rows as read from / written to the system tables
namespace CoreEngine::Catalog{
    using Schemas::ConstraintType;

    // TODO: runtime index description, replaced by Schemas::IndexSchema
    struct Index{
        using IndexedColumns = DataStructures::StaticArray<UInt8_t, 10>;
        IndexedColumns columns;

        explicit Index(const UInt8_t* columns, const Int32_t size)
            : columns(columns, size){}

        Index() = default;
    };

    struct AuditInformation{
        DataTypes::DateTime createdAt;
        DataTypes::DateTime lastModified;
        DataTypes::String lastModifiedBy;
        Int32_t version;
        bool isDeleted;
        DataTypes::DateTime deletedAt;

        explicit AuditInformation()
            :   createdAt(DataTypes::DateTime::Now()),
                lastModified(DataTypes::DateTime::Now()),
                lastModifiedBy(DataTypes::String::Null()),
                version(0), isDeleted(false),
                deletedAt(DataTypes::DateTime::Now()){}

        explicit AuditInformation(
            const DataTypes::DateTime& createdAt,
            const DataTypes::DateTime& lastModified,
            const DataTypes::String& lastModifiedBy,
            const Int32_t version,
            const bool isDeleted,
            const DataTypes::DateTime& deletedAt
        ) : createdAt(createdAt),
            lastModified(lastModified),
            lastModifiedBy(lastModifiedBy),
            version(version),
            isDeleted(isDeleted),
            deletedAt(deletedAt){}

        // Partial constructor — for headers that only store version/isDeleted/deletedAt
        explicit AuditInformation(
            const Int32_t version,
            const bool isDeleted,
            const DataTypes::String& lastModifiedBy,
            const DataTypes::DateTime& deletedAt
        ) : createdAt(DataTypes::DateTime::Now()),
            lastModified(DataTypes::DateTime::Now()),
            lastModifiedBy(lastModifiedBy),
            version(version),
            isDeleted(isDeleted),
            deletedAt(deletedAt){}
    };

    struct IndexColumnsHeader{
        Int32_t indexId = INVALID_INDEX_ID;
        Int32_t columnId;
        Int16_t ordinalPosition;
        bool isIncluded;

        AuditInformation additionalInfo;
    };

    struct IdentityColumnsHeader{
        Int32_t tableId;
        Int32_t columnId;
        Int32_t seedValue;
        Int32_t increment;
        Int64_t lastValue;

        bool isCached;
        Int32_t cacheBlock;
        AuditInformation additionalInfo;

        explicit IdentityColumnsHeader()
            :   tableId(INVALID_TABLE_ID), columnId(INVALID_COLUMN_ID),
                seedValue(0), increment(1), lastValue(0),
                isCached(false), cacheBlock(0){}

        explicit IdentityColumnsHeader(
            const Int32_t tableId,
            const Int32_t columnId,
            const Int32_t seedValue,
            const Int32_t increment,
            const Int64_t lastValue,
            const bool isCached,
            const Int32_t cacheBlock
        ):  tableId(tableId),columnId(columnId),
            seedValue(seedValue), increment(increment),
            lastValue(lastValue), isCached(isCached),
            cacheBlock(cacheBlock){}

        explicit IdentityColumnsHeader(
            const Int32_t tableId,
            const Int32_t columnId,
            const Int32_t seedValue,
            const Int32_t increment,
            const Int64_t lastValue,
            const bool isCached,
            const Int32_t cacheBlock,
            AuditInformation  additionalInfo
        ):  tableId(tableId), columnId(columnId),
            seedValue(seedValue), increment(increment),
            lastValue(lastValue),isCached(isCached),
            cacheBlock(cacheBlock),additionalInfo(std::move(additionalInfo)){}
    };

    struct IndexHeader {
        Int32_t tableId;
        Int32_t id = -1;
        DataTypes::String name;
        bool isClustered;
        bool isDisabled;
        AuditInformation additionalInfo;

        DataStructures::PolymorphicArray<IndexColumnsHeader> columns;
    };

    struct ConstraintsColumnsHeader{
        Int32_t constraintId = INVALID_CONSTRAINT_ID;
        Int32_t columnId;
        Int32_t ordinalPosition;
        AuditInformation additionalInfo;
    };

    struct ConstraintsHeader{
        Int32_t tableId;
        Int32_t constraintId = INVALID_CONSTRAINT_ID;
        DataTypes::String name;
        ConstraintType type;
        bool isDisabled;
        Int32_t indexId = INVALID_INDEX_ID;
        IndexHeader index;
        DataStructures::PolymorphicArray<ConstraintsColumnsHeader> columns;

        AuditInformation additionalInfo;
    };

    struct DefaultValuesHeader {
        Int32_t columnId = INVALID_COLUMN_ID;
        DataTypes::String value;
        //add size here

        AuditInformation additionalInfo;
    };

    struct TableStatistics {
        Int32_t tableId ;

        Int64_t rowCount;
        Int32_t averageRowSize;
        Int32_t pageCount;

        DataTypes::DateTime lastModified;

        TableStatistics() {
            this->tableId = INVALID_TABLE_ID;
            this->rowCount = 0;
            this->averageRowSize = 0;
            this->pageCount = 0;
            this->lastModified = DataTypes::DateTime::Now();
        }

        explicit TableStatistics(const Int32_t tableId)
            : tableId(tableId),
              rowCount(0),averageRowSize(0), pageCount(0), lastModified(DataTypes::DateTime::Now()) {}

        TableStatistics(
            const Int32_t tableId,
            const Int64_t& rowCount,
            const Int32_t averageRowSize,
            const Int32_t pageCount,
            const DataTypes::DateTime& lastModified
        ) : tableId(tableId),
            rowCount(rowCount),
            averageRowSize(averageRowSize),
            pageCount(pageCount),
            lastModified(lastModified) {}
    };

    struct ColumnStatistics {
        Int32_t columnId;

        Int64_t distinctCount;
        Value min;
        Value max;
        Int64_t nullCount;
    };

    struct ColumnHistograms {
        Int32_t columnId;
        Int32_t histogramId;

        Value rangeStart;
        Value rangeEnd;

        Int32_t rowCount;
        Int32_t distinctCount;

        ColumnHistograms(
            const Int32_t columnId,
            const Value& rangeStart,
            const Value& rangeEnd,
            const Int32_t rowCount,
            const Int32_t distinctCount
        )
            : columnId(columnId),
              rangeStart(rangeStart),
              rangeEnd(rangeEnd),
              rowCount(rowCount),
              distinctCount(distinctCount)
        {
            this->histogramId = INVALID_HISTOGRAM_ID;
        }

        ColumnHistograms(
            const Int32_t columnId,
            const Int32_t histogramId,
            const Value& rangeStart,
            const Value& rangeEnd,
            const Int32_t rowCount,
            const Int32_t distinctCount
        )
            : columnId(columnId),
              histogramId(histogramId),
              rangeStart(rangeStart),
              rangeEnd(rangeEnd),
              rowCount(rowCount),
              distinctCount(distinctCount){}
    };

    struct IndexStatistics {
        Int32_t tableId;
        Int32_t indexId;
        Int32_t leafPages;
        Int8_t depth;
        DataTypes::Decimal averageFragmentation;
        DataTypes::DateTime lastUpdated;

        IndexStatistics() {
            this->tableId = INVALID_TABLE_ID;
            this->indexId = INVALID_INDEX_ID;
            this->leafPages = 0;
            this->depth = 0;
            this->averageFragmentation = DataTypes::Decimal(0);
            this->lastUpdated = DataTypes::DateTime::Now();
        }

        IndexStatistics(
            const Int32_t tableId,
            const Int32_t indexId
        ) : IndexStatistics() {
            this->tableId = tableId;
            this->indexId = indexId;
        }

        IndexStatistics(
            const Int32_t tableId,
            const Int32_t indexId,
            const Int32_t leafPages,
            const Int8_t& depth,
            const DataTypes::Decimal& averageFragmentation,
            const DataTypes::DateTime& lastUpdated
        )
            : tableId(tableId),
              indexId(indexId),
              leafPages(leafPages),
              depth(depth),
              averageFragmentation(averageFragmentation),
              lastUpdated(lastUpdated) {}

        void Reset() {
            this->depth = 0;
            this->leafPages = 0;
            this->averageFragmentation = DataTypes::Decimal(0);
        }
    };

    struct ColumnHeader {
        Int32_t tableId;
        Int32_t id = INVALID_COLUMN_ID;
        DataTypes::String name;
        UInt8_t dataType;
        Int32_t recordSize;
        Int8_t precision = INVALID_DECIMAL_PRECISION;
        Int8_t scale = INVALID_DECIMAL_SCALE;
        bool isNullable;
        Int16_t ordinalPosition;
        bool isSystem;

        AuditInformation additionalInfo;
    };

    struct TableHeader {
        Int32_t databaseId;
        Int32_t id;
        Int32_t schemaId;
        DataTypes::String name;
        Int16_t ordinalPosition;
        bool isSystem;

        AuditInformation additionalInfo;

        TableStatistics statistics;

        DataStructures::PolymorphicArray<ColumnHeader> columns;
        DataStructures::PolymorphicArray<ConstraintsHeader> constraints;

        DataStructures::PolymorphicArray<IdentityColumnsHeader> identity;

        TableHeader()
            : databaseId(INVALID_DATABASE_ID), id(INVALID_TABLE_ID), schemaId(INVALID_SCHEMA_ID), ordinalPosition(0), isSystem(false){}
    };

    struct SchemaHeader {
        Int32_t id = -1;
        Int32_t databaseId;
        DataTypes::String name;
        AuditInformation additionalInfo;

        SchemaHeader(){
            this->id = INVALID_SCHEMA_ID;
            this->databaseId = INVALID_DATABASE_ID;
        }

        SchemaHeader(
            const Int32_t id,
            const Int32_t databaseId,
            const DataTypes::String& name
        ) : id(id), databaseId(databaseId), name(name){}

    };

    struct TableDefinition{
        const TableHeader* _table;
        DataStructures::PolymorphicArray<ColumnHeader> _columns;
        DataStructures::PolymorphicArray<IndexHeader> _indexes;
        DataStructures::PolymorphicArray<ConstraintsHeader> _constraints;
        DataStructures::PolymorphicArray<IdentityColumnsHeader> _identities;
        DataStructures::PolymorphicArray<DefaultValuesHeader> _defaults;

        UInt32_t _indexesColumnsCount;
        UInt32_t _constraintsColumnsCount;
    };

    struct DatabaseHeader {
        Int32_t id = -1;
        DataTypes::String name;
        DataTypes::String filepath;
        bool isSystem;
        AuditInformation additionalInfo;

        DataStructures::PolymorphicArray<TableHeader> tables;
        DataStructures::PolymorphicArray<SchemaHeader> schemas;
    };
}