#include "../include/RowCursor.h"

#include "PhysicalPlan.h"
#include "Vectorization/Vectorization.h"

namespace QueryPipeline{
    RowCursor::RowCursor(const PhysicalPlan::VectorBatch* batch)
        : _batch(batch) {}

    void RowCursor::PrintRows(std::ostream& os) const{
        for (auto i = 0;i < this->_batch->_numberOfRows; i++){
            for (auto j = 0; j < this->_batch->_numberOfColumns; j++)
                this->PrintColumn(os, i, j);
            os << std::endl;
        }
    }

    void RowCursor::PrintColumn(std::ostream& os, const Int rowIndex, const Int columnIndex) const{
        const auto* columnData = this->_batch->_columns[columnIndex];
        switch (columnData->_type){
        case DataType::String:
            break;
        case DataType::Bool:
            os << (*reinterpret_cast<const bool*>(columnData->_data + rowIndex * sizeof(bool)) == 1 ? "true" : "false");
            break;
        case DataType::TinyInt:
            os << (*reinterpret_cast<const TinyInt*>(columnData->_data + rowIndex * sizeof(TinyInt)));
            break;
        case DataType::SmallInt:
            os << (*reinterpret_cast<const SmallInt*>(columnData->_data + rowIndex * sizeof(SmallInt)));
            break;
        case DataType::Int:
            os << (*reinterpret_cast<const Int*>(columnData->_data + rowIndex * sizeof(Int)));
            break;
        case DataType::BigInt:
            os << (*reinterpret_cast<const BigInt*>(columnData->_data + rowIndex * sizeof(BigInt)));
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
            os << "NULL";
            break;
        case DataType::RowIdentifier:
            break;
        }

        os << " || ";
    }

    std::ostream& operator<<(std::ostream& os, const RowCursor& cursor){
        cursor.PrintRows(os);
        return os;
    }
}
