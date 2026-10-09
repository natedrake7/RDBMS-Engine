#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Messages.h>

namespace CoreEngine::StorageTypes{

    Errors::RuntimeStatus Table::ValidateTableLayout(const ::Memory::IAllocator* allocator) const{
        UnsignedInt minimum = sizeof(RowHeader) + this->ColumnCount() * sizeof(RowEntry);

        for (const auto& column : this->Columns()){
            minimum += this->CanStoreColumnOffRow(column._ordinalPosition)
                    ? LOB_REFERENCE_SIZE
                    : column._recordSize;
        }

        if (minimum > this->MaxInlineRowSize()){
            return Errors::RuntimeStatus(
                Errors::RuntimeError::ColumnSizeExceeded,
                Messages::TABLE_LAYOUT_TOO_LARGE,
                allocator
            );
        }

        return Errors::RuntimeStatus();
    }

    row_size_t Table::MaxInlineRowSize() const{
        if (this->IsClustered()){
            return static_cast<row_size_t>(
                (Constants::INDEX_PAGE_DEFAULT_SIZE / (2 * Indexing::BTree::MIN_TREE_DEGREE))
                - this->ClusteredKeySize()
                - Pages::SlotDirectory::SIZE
            );
        }

        return Constants::PAGE_SIZE_WITHOUT_HEADER - Pages::SlotDirectory::SIZE;
    }

    row_size_t Table::ClusteredKeySize() const{
        row_size_t size = 0;
        const auto* index = &this->_schema->_indexes[this->_schema->_clusteredIndexOrdinalPosition];
        for (Int i = 0;i < index->_keyCount; i++)
            size += this->_schema->_columns[index->_keyColumns[i]._ordinalPosition]._recordSize;
        return size;
    }

    row_size_t Table::WorstCaseRowSize() const{
        UnsignedInt size = sizeof(RowHeader) + sizeof(RowEntry) * this->ColumnCount();

        for (const auto& column : this->Columns()){
            size += column._recordSize;
        }

        return static_cast<row_size_t>(Math::Min<UnsignedInt>(size, this->MaxInlineRowSize()));
    }

    bool Table::IsKeyColumn(const column_index_t ordinalPosition) const{
        auto* column = &this->_schema->_columns[ordinalPosition];

        for (Int i = 0;i < this->_schema->_indexesCount; i++){
            const auto* index = &this->_schema->_indexes[i];
            if (index->_coveredColumnsMask.HasValue(ordinalPosition))
                return true;
        }

        return false;
    }

    bool Table::CanStoreColumnOffRow(const column_index_t ordinalPosition) const{
        const auto* column = &this->_schema->_columns[ordinalPosition];
        const auto type = column->_type;

        return (type == DataType::String || type == DataType::Json)
            && column->_recordSize > LOB_REFERENCE_SIZE
            && !this->IsKeyColumn(ordinalPosition);
    }

    const ::Memory::IAllocator* Table::Allocator() const{
        return &this->_allocator;
    }
}
