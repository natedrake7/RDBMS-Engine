#include <QueryPipeline/RowCursor.h>

#include <QueryPipeline/PhysicalPlan.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/DataTypes/StringValue.h>
#include <Systemic/DataTypes/JsonBinary.h>

namespace QueryPipeline{
    RowCursor::RowCursor(const CoreEngine::DataChunk* batch)
        : _batch(batch) {}

    void RowCursor::PrintRows(std::ostream& os) const{
        for (auto rowIndex = 0;rowIndex < this->_batch->_numberOfRows; rowIndex++){
            for (auto columnIndex = 0; columnIndex < this->_batch->_numberOfColumns; columnIndex++)
                this->PrintColumn(os, rowIndex, columnIndex);
            os << std::endl;
        }
    }

    void RowCursor::PrintColumn(std::ostream& os, const Int32_t rowIndex, const Int32_t columnIndex) const{
        const auto* columnData = this->_batch->_columns[columnIndex];

        if (columnData->GetNullValue(rowIndex)){
            os  << "NULL"
                << " || ";
            return;
        }

        switch (columnData->_type){
        case DataType::String:{
            // SlotAt strides by the vector's own entry size: a String column stores
            // 16-byte StringValue entries, not 24-byte String ones.
            const auto* str = reinterpret_cast<const DataTypes::StringValue*>(columnData->SlotAt(rowIndex));
            os << *str;
            break;
        }
        case DataType::Bool:
            os << (*reinterpret_cast<const bool*>(columnData->_data + rowIndex * sizeof(bool)) == 1 ? "TRUE" : "FALSE");
            break;
        case DataType::TinyInt:
            os << static_cast<Int32_t>((*reinterpret_cast<const Int8_t*>(columnData->_data + rowIndex * sizeof(Int8_t))));
            break;
        case DataType::SmallInt:
            os << (*reinterpret_cast<const Int16_t*>(columnData->_data + rowIndex * sizeof(Int16_t)));
            break;
        case DataType::Int:
            os << (*reinterpret_cast<const Int32_t*>(columnData->_data + rowIndex * sizeof(Int32_t)));
            break;
        case DataType::BigInt:
            os << (*reinterpret_cast<const Int64_t*>(columnData->_data + rowIndex * sizeof(Int64_t)));
            break;
        case DataType::Decimal:{
            os << (*reinterpret_cast<const DataTypes::Decimal*>(columnData->_data + rowIndex * sizeof(DataTypes::Decimal)));
            break;
        }
        case DataType::DateTime:{
            const auto time = DataTypes::DateTime(*reinterpret_cast<const Int64_t*>(columnData->_data + rowIndex * sizeof(Int64_t)));
            time.Print(os);
            break;
        }
        case DataType::Guid:{
            const auto guid = DataTypes::Guid(columnData->_data + rowIndex * sizeof(DataTypes::Guid));
            os << guid;
            break;
        }
        case DataType::Json:{
            auto* json = reinterpret_cast<const DataTypes::JsonBinary*>(columnData->_data + rowIndex * sizeof(DataTypes::JsonBinary));
            os << json->ToString();
            break;
        }
        default:
        case DataType::Null:
            break;
        }

        os << " || ";
    }

    std::ostream& operator<<(std::ostream& os, const RowCursor& cursor){
        cursor.PrintRows(os);
        return os;
    }
}
