#include <CoreEngine/DataStorage/Row/SerializedRow.h>

#include <Systemic/DataTypes/Value.h>
#include <Systemic/IntegerConverter.h>
#include <CoreEngine/Contexts/ExecutionContext.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/Pages/Additional/RawRowReference.h>

namespace CoreEngine::StorageTypes{
    SerializedRow::SerializedRow(){
        this->_data = nullptr;
        this->capacity = 0;
        this->offset = 0;
    }

    SerializedRow::SerializedRow(
        object_t* buffer,
        const UInt16_t capacity,
        const UInt16_t startingOffset
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

    UInt16_t SerializedRow::SetData(const void* otherData, const UInt16_t dataSize){
        std::memcpy(this->_data + this->offset, otherData, dataSize);
        const auto dataOffset = this->offset;
        this->offset += dataSize;
        return dataOffset;
    }

    void SerializedRow::SetData(const void* otherData, const UInt16_t dataSize, const Int32_t offSet) const{
        std::memcpy(this->_data + offSet, otherData, dataSize);
    }

    void SerializedRow::AlignSizeWithOffset(){
        this->capacity = this->offset;
    }

    Value SerializedRow::MaterializeColumn(
        const ExecutionContext& context,
        const Schemas::ColumnSchema* column
    ) const{
        // Calculate bitmap size once
        const auto columnOrdinal = column->_ordinalPosition;

        const auto offSet = sizeof(RowHeader) + columnOrdinal * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);

        if (columnDataEntry->Type() == RowEntry::NULLVAL)
            return Value::Null();

        // Create and populate the value
        return Value::FromExternalStorage(
            this->_data + columnDataEntry->Offset(),
            columnDataEntry->Size(),
            column->_type,
            context.GetAllocator(),
            columnOrdinal
        );
    }

    object_t* SerializedRow::ColumnAt(const column_index_t index, Int32_t& outSize) const{
        const auto offSet = sizeof(RowHeader) + index * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);
        outSize = columnDataEntry->Size();
        return this->_data + columnDataEntry->Offset();
    }

    Int32_t SerializedRow::ColumnSize(const column_index_t index) const{
        const auto offSet = sizeof(RowHeader) + index * sizeof(RowEntry);
        const auto* columnDataEntry = reinterpret_cast<const RowEntry*>(this->_data + offSet);
        return columnDataEntry->Size();
    }

    object_t* SerializedRow::Data() const{
        return this->_data;
    }

    UInt16_t SerializedRow::Size() const{
        return this->capacity;
    }

    UInt16_t SerializedRow::Offset() const{
        return this->offset;
    }
}
