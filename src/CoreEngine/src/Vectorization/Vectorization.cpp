#include "../../include/Vectorization/Vectorization.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->selectedRids[index] = static_cast<UnsignedInt*>(allocator->AllocateRaw(size * sizeof(UnsignedInt)));
        this->isIdentity = false;
    }

    DataVector::DataVector(const DataType type)
        :   _data{nullptr}, _validity{nullptr},
            _count(0), _type(type),
            _kind(DataVectorKind::Flat),
            _dataEntrySize(0){}

    object_t* DataVector::SlotAt(const Int index) const{
        return this->_data + this->_dataEntrySize * index;
    }

    void DataVector::SetNullValue(const Int index, const bool value) const{
        const Int w = index >> 6;
        const UnsignedBigInt m = 1ull << (index & 63);
        this->_validity[w] = ( this->_validity[w] & ~m) | (static_cast<uint64_t>(value) << (index & 63));
    }

    bool DataVector::GetNullValue(const Int index) const{
        return this->_validity[index >> 6] >> (index & 63) & 1;
    }

    DataVector* DataVector::FlatVector(
        const Memory::IAllocator* allocator,
        DataType type,
        const Int count
    ){
        auto* dataVector = allocator->Allocate<DataVector>(type);
        dataVector->_count = count;
        dataVector->_kind  = DataVectorKind::Flat;
        dataVector->_type = type;
        dataVector->_dataEntrySize = VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<Int>(type)];
        dataVector->_data  = static_cast<object_t*>(allocator->AllocateRaw(dataVector->_dataEntrySize * count));

        const Int validityWords = (count + 63) / 64;
        dataVector->_validity = static_cast<UnsignedBigInt*>(allocator->AllocateRaw(validityWords * sizeof(UnsignedBigInt)));
        std::memset(dataVector->_validity, 0, validityWords * sizeof(UnsignedBigInt));   // 0 = not-null default
        return dataVector;
    }

    DataChunk::DataChunk()
        :   _columns(nullptr), _selection(nullptr),
            _numberOfColumns(0), _numberOfRows(0) {}

    DataChunk::DataChunk(DataChunk&& other) noexcept
        : _columns(other._columns), _selection(other._selection),
            _numberOfColumns(other._numberOfColumns), _numberOfRows(other._numberOfRows){
    }

    DataChunk& DataChunk::operator=(DataChunk&& other) noexcept{
        if (this == &other)
            return *this;

        this->_columns = other._columns;
        this->_selection = other._selection;
        this->_numberOfColumns = other._numberOfColumns;
        this->_numberOfRows = other._numberOfRows;
        return *this;
    }

    void DataChunk::AllocateColumns(const Memory::IAllocator* allocator, const Int numberOfColumns){
        this->_columns = static_cast<DataVector**>(
            allocator->AllocateRaw(sizeof(DataVector*)*numberOfColumns)
        );
    }

    void DataChunk::SetColumn(DataVector* columnData, const Int columnIndex) const{
        this->_columns[columnIndex] = columnData;
    }

    Int DataChunk::RowPhysicalIndex(const Int rowLogicalIndex) const{
        return this->_selection == nullptr
            ? rowLogicalIndex
            : this->_selection[rowLogicalIndex];
    }
}
