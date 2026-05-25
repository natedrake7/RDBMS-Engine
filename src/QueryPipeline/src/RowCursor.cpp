#include "../include/RowCursor.h"

#include "PhysicalPlan.h"
#include "Vectorization/Vectorization.h"

namespace QueryPipeline{
    RowCursor::RowCursor(const PhysicalPlan::VectorBatch* batch)
        : _batch(batch), _currentRow(0) {}

    void RowCursor::PrintRows(std::ostream& os) const{
        for (int i = 0;i < this->_batch->_numberOfRows; i++){
            for (int j = 0; i < this->_batch->_numberOfColumns; j++){
                this->PrintColumn(os, j);
            }
        }
    }

    void RowCursor::PrintColumn(std::ostream& os, const Int columnIndex) const{
        const auto* columnData = this->_batch->_columns[columnIndex];
        switch (columnData->_type){
        case DataType::String:
            break;
        case DataType::Bool:
            os << (*reinterpret_cast<const bool*>(columnData->_data + this->_currentRow * sizeof(bool)) == 1 ? "true" : "false");
            break;
        case DataType::TinyInt:
            os << (*reinterpret_cast<const TinyInt*>(columnData->_data + this->_currentRow * sizeof(TinyInt)));
            break;
        case DataType::SmallInt:
            os << (*reinterpret_cast<const SmallInt*>(columnData->_data + this->_currentRow * sizeof(SmallInt)));
            break;
        case DataType::Int:
            os << (*reinterpret_cast<const Int*>(columnData->_data + this->_currentRow * sizeof(Int)));
            break;
        case DataType::BigInt:
            os << (*reinterpret_cast<const BigInt*>(columnData->_data + this->_currentRow * sizeof(BigInt)));
            break;
        case DataType::Decimal:
            break;
        case DataType::DateTime:
            break;
        case DataType::Guid:
            break;
        case DataType::Json:
            break;
        case DataType::Null:
            break;
        case DataType::RowIdentifier:
            break;
        }
    }

    std::ostream& operator<<(std::ostream& os, const RowCursor& cursor){
        cursor.PrintRows(os);
        return os;
    }
}
