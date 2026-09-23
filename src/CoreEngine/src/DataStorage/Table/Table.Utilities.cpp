#include "../../../include/DataStorage/Table.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/Messages.h"

namespace CoreEngine::StorageTypes{

    Errors::RuntimeStatus Table::ValidateTableLayout(const ::Memory::IAllocator* allocator) const{
        UnsignedInt minimum = sizeof(RowHeader) + this->_columns.Size() * sizeof(RowEntry);

        for (const auto* column : this->_columns){
            minimum += this->CanStoreColumnOffRow(column->OrdinalPosition())
                    ? LOB_REFERENCE_SIZE
                    : column->Size();
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
        for (const auto pos : this->clusteredHeader.columns)
            size += this->_columns[pos]->Size();
        return size;
    }

    row_size_t Table::WorstCaseRowSize() const{
        UnsignedInt size = sizeof(RowHeader) + sizeof(RowEntry) * this->_columns.Size();

        for (const auto* column : this->_columns){
            size += column->Size();
        }

        return static_cast<row_size_t>(Math::Min<UnsignedInt>(size, this->MaxInlineRowSize()));
    }

    bool Table::IsKeyColumn(const column_index_t ordinalPosition) const{
        for (const auto pos : this->clusteredHeader.columns)
            if (pos == ordinalPosition)
                return true;

        for (Int i = 0; i < this->nonClusteredHeaders.Size(); i++){
            for (const auto pos : this->nonClusteredHeaders[i].columns)
                if (pos == ordinalPosition)
                    return true;
        }

        return false;
    }

    bool Table::CanStoreColumnOffRow(const column_index_t ordinalPosition) const{
        const auto* column = this->_columns[ordinalPosition];
        const auto type = column->Type();
        return (type == DataType::String || type == DataType::Json)
            && column->Size() > LOB_REFERENCE_SIZE
            && !this->IsKeyColumn(ordinalPosition);
    }
}