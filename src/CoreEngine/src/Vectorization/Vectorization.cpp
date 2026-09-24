#include "../../include/Vectorization/Vectorization.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->selectedRids[index] = static_cast<UnsignedInt*>(allocator->AllocateRaw(size * sizeof(UnsignedInt)));
        this->isIdentity = false;
    }

    DataVector::DataVector(const DataType type)
        :   _data(nullptr), _validity(nullptr),
            _selection(nullptr),
            _count(0), _type(type),
            _kind(DataVectorKind::Flat),
            _dataEntrySize(0){}


    void DataVector::SetNullValue(const UnsignedInt index, const bool value) const{
        EngineBitmap::SetBitmapBit(this->_validity, index, value);
    }

    UnsignedInt DataVector::PhysicalIndex(const UnsignedInt logicalIndex) const{
        switch (this->_kind){
        case DataVectorKind::Flat:
            return logicalIndex;
        case DataVectorKind::Constant:
            return 0;
        case DataVectorKind::Dictionary:
            return this->_selection[logicalIndex];
        }

        throw std::runtime_error("Invalid DataVector kind");
    }

    UnsignedInt DataVector::DictionaryIndex(const UnsignedInt logicalIndex) const{
        return this->_selection[logicalIndex];
    }

    Int DataVector::ValidityBytes() const{
        return this->WordsCount() * sizeof(UnsignedBigInt);
    }

    Int DataVector::WordsCount() const{
        return DataVector::WordsCount(this->_count);
    }

    Int DataVector::WordsCount(const Int count){
        return (count + 63) / 64;
    }

    Int DataVector::BitSizeFromBool(const Int count){
        return (count * sizeof(bool) + 7) / 8;
    }

    DataVector* DataVector::FlatVector(
        const Memory::IAllocator* allocator,
        const DataType type,
        const Int count
    ){
        auto* dataVector = allocator->Allocate<DataVector>(type);
        dataVector->_count = count;
        dataVector->_kind  = DataVectorKind::Flat;
        dataVector->_dataEntrySize = VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<Int>(type)];
        dataVector->_data  = static_cast<object_t*>(allocator->AllocateRaw(dataVector->_dataEntrySize * count));
        std::memset(dataVector->_data, 0, dataVector->_dataEntrySize * count);

        const auto validityWords = dataVector->WordsCount();
        dataVector->_validity = static_cast<UnsignedBigInt*>(allocator->AllocateRaw(validityWords * sizeof(UnsignedBigInt)));
        std::memset(dataVector->_validity, 0, validityWords * sizeof(UnsignedBigInt));   // 0 = not-null default
        return dataVector;
    }

    DataVector* DataVector::ConstantVector(
        const Memory::IAllocator* allocator,
        const bool isNull,
        DataType type
    ){
        auto* dataVector = allocator->Allocate<DataVector>(type);
        dataVector->_count = 1;
        dataVector->_kind  = DataVectorKind::Constant;
        dataVector->_type = type;
        dataVector->_dataEntrySize = VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<Int>(type)];
        dataVector->_data  = static_cast<object_t*>(allocator->AllocateRaw(dataVector->_dataEntrySize));
        std::memset(dataVector->_data, 0, dataVector->_dataEntrySize);

        dataVector->_validity = static_cast<UnsignedBigInt*>(allocator->AllocateRaw(sizeof(UnsignedBigInt)));
        dataVector->_validity[0] = isNull
            ? ~0ull
            : 0ull;
        return dataVector;
    }

    void DataVector::SetAllNull() const{
        std::memset(this->_validity, 0xFF, this->WordsCount() * sizeof(UnsignedBigInt));
    }

    void DataVector::CopyValidity(const DataVector* other) const{
        std::memcpy(this->_validity, other->_validity, this->WordsCount() * sizeof(UnsignedBigInt));
    }

    void DataVector::OrValidity(const DataVector* lhs, const DataVector* rhs) const{
        for (Int i = 0; i < lhs->WordsCount(); ++i)
            this->_validity[i] |= lhs->_validity[i] | rhs->_validity[i];
    }

    void DataVector::ConvertToDictionary(const UnsignedInt* selection){
        this->_kind = DataVectorKind::Dictionary;
        this->_selection = selection;
    }

    bool DataVector::IsConstant() const{ return this->_kind == DataVectorKind::Constant; }

    bool DataVector::IsFlat() const{ return this->_kind == DataVectorKind::Flat; }

    bool DataVector::IsDictionary() const{ return this->_kind == DataVectorKind::Dictionary; }

    DataChunk::DataChunk()
        :   _columns(nullptr), _selection(nullptr),
            _numberOfColumns(0), _numberOfRows(0) {}

    DataChunk::DataChunk(DataChunk&& other) noexcept
        : _columns(other._columns), _selection(other._selection),
            _numberOfColumns(other._numberOfColumns), _numberOfRows(other._numberOfRows){
        other._columns = nullptr;
        other._selection = nullptr;
        other._numberOfColumns = 0;
        other._numberOfRows = 0;
    }

    DataChunk& DataChunk::operator=(DataChunk&& other) noexcept{
        if (this == &other)
            return *this;

        this->_columns = other._columns;
        this->_selection = other._selection;
        this->_numberOfColumns = other._numberOfColumns;
        this->_numberOfRows = other._numberOfRows;

        other._columns = nullptr;
        other._selection = nullptr;
        other._numberOfColumns = 0;
        other._numberOfRows = 0;
        return *this;
    }

    void DataChunk::AllocateColumns(const Memory::IAllocator* allocator, const Int numberOfRows, const Int numberOfColumns){
        this->_columns = static_cast<DataVector**>(
            allocator->AllocateRaw(sizeof(DataVector*)*numberOfColumns)
        );

        this->_numberOfRows = numberOfRows;
        this->_numberOfColumns = numberOfColumns;
    }

    void DataChunk::SetColumn(DataVector* columnData, const Int columnIndex) const{
        this->_columns[columnIndex] = columnData;
    }

    Int DataChunk::RowPhysicalIndex(const Int rowLogicalIndex) const{
        return this->_selection == nullptr
            ? rowLogicalIndex
            : static_cast<Int>(this->_selection[rowLogicalIndex]);
    }
}
