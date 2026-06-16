#include "../include/RowCursor.h"

#include "PhysicalPlan.h"
#include "Vectorization/Vectorization.h"
#include "../../Systemic/include/DataTypes/String.h"
#include "../../Systemic/include/DataTypes/JsonBinary.h"

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

        if (columnData->GetNullValue(rowIndex)){
            os  << "NULL"
                << " || ";
            return;
        }

        switch (columnData->_type){
        case DataType::String:{
            auto* str = reinterpret_cast<const DataTypes::String*>(columnData->_data + rowIndex * sizeof(DataTypes::String));
            os << *str;
            break;
        }
        case DataType::Bool:
            os << (*reinterpret_cast<const bool*>(columnData->_data + rowIndex * sizeof(bool)) == 1 ? "TRUE" : "FALSE");
            break;
        case DataType::TinyInt:
            os << static_cast<Int>((*reinterpret_cast<const TinyInt*>(columnData->_data + rowIndex * sizeof(TinyInt))));
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
        case DataType::DateTime:{
            const auto time = DataTypes::DateTime(*reinterpret_cast<const BigInt*>(columnData->_data + rowIndex * sizeof(BigInt)));
            time.Print(os);
            break;
        }
        case DataType::Guid:
            // const auto guid = DataTypes::Guid(*reinterpret_cast<const UInt*>(columnData->_data + rowIndex * sizeof(UInt)));
            break;
        case DataType::Json:{
            auto* json = reinterpret_cast<const DataTypes::JsonBinary*>(columnData->_data + rowIndex * sizeof(DataTypes::JsonBinary));
            os << json->ToString();
            break;
        }
        default:
        case DataType::Null:
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
