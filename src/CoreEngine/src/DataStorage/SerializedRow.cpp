#include "../../include/DataStorage/SerializedRow.h"

#include <cmath>

#include "Converter.h"
#include "Contexts/ExecutionContext.h"
#include "DataStorage/Column.h"
#include "DataStorage/Row.h"
#include "Pages/Additional/RawRowReference.h"

namespace CoreEngine::StorageTypes{
    SerializedRow::SerializedRow(){
        this->_data = nullptr;
        this->capacity = 0;
        this->offset = 0;
    }

    SerializedRow::SerializedRow(
        object_t* buffer,
        const UnsignedSmallInt capacity,
        const UnsignedSmallInt startingOffset
    ){
        this->_data = buffer;
        this->capacity = capacity;
        this->offset = startingOffset;
    }

    SerializedRow& SerializedRow::operator=(SerializedRow&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->capacity = other.capacity;
        this->offset = other.offset;

        other._data = nullptr;
        other.capacity = 0;
        other.offset = 0;

        return *this;
    }

    SerializedRow::SerializedRow(SerializedRow&& other) noexcept{
        this->_data = other._data;
        this->capacity = other.capacity;
        this->offset = other.offset;

        other._data = nullptr;
        other.capacity = 0;
        other.offset = 0;
    }

    SerializedRow SerializedRow::FromRowPtr(const Pages::RawRowReference& rowPtr){
        auto payload = SerializedRow();

        payload._data = rowPtr._data;
        payload.capacity = rowPtr.size;
        payload.offset = 0;

        return payload;
    }

    UnsignedSmallInt SerializedRow::SetData(const void* otherData, const UnsignedSmallInt dataSize){
        std::memcpy(this->_data + this->offset, otherData, dataSize);
        const auto dataOffset = this->offset;
        this->offset += dataSize;
        return dataOffset;
    }

    void SerializedRow::SetData(const void* otherData, const UnsignedSmallInt dataSize, const Int offSet) const{
        std::memcpy(this->_data + offSet, otherData, dataSize);
    }

    void SerializedRow::AlignSizeWithOffset(){
        this->capacity = this->offset;
    }

    Value SerializedRow::MaterializeColumn(
        const ExecutionContext& context,
        const Column* column
    ) const{
        // Calculate bitmap size once
        const auto columnOrdinal = column->OrdinalPosition();

        const auto offSet = sizeof(RowHeader) + columnOrdinal * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);

        if (columnDataEntry->Type() == RowEntry::NULLVAL)
            return Value::Null(context.GetAllocator());

        // Create and populate the value
        return Value::FromExternalStorage(
            this->_data + columnDataEntry->Offset(),
            columnDataEntry->Size(),
            column->Type(),
            context.GetAllocator(),
            columnOrdinal
        );
    }

    object_t* SerializedRow::ColumnAt(const column_index_t index, Int& outSize) const{
        const auto offSet = sizeof(RowHeader) + index * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);
        outSize = columnDataEntry->Size();
        return this->_data + columnDataEntry->Offset();
    }

    Int SerializedRow::ColumnSize(const column_index_t index) const{
        const auto offSet = sizeof(RowHeader) + index * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);
        return columnDataEntry->Size();
    }

    object_t* SerializedRow::Data() const{
        return this->_data;
    }

    UnsignedSmallInt SerializedRow::Size() const{
        return this->capacity;
    }

    UnsignedSmallInt SerializedRow::Offset() const{
        return this->offset;
    }
}
