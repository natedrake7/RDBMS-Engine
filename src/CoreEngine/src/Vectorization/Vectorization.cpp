#include <CoreEngine/Vectorization/Vectorization.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int32_t index, const Int32_t size){
        this->selectedRids[index] = static_cast<UInt32_t*>(allocator->AllocateRaw(size * sizeof(UInt32_t)));
        this->isIdentity = false;
    }

    DataVector::DataVector(const DataType type)
        :   _data(nullptr), _validity(nullptr),
            _selection(nullptr),
            _count(0), _type(type),
            _kind(DataVectorKind::Flat),
            _dataEntrySize(0){}


    void DataVector::SetNullValue(const UInt32_t index, const bool value) const{
        EngineBitmap::SetBitmapBit(this->_validity, index, value);
    }

    UInt32_t DataVector::PhysicalIndex(const UInt32_t logicalIndex) const{
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

    UInt32_t DataVector::DictionaryIndex(const UInt32_t logicalIndex) const{
        return this->_selection[logicalIndex];
    }

    UInt32_t DataVector::ValidityBytes() const{
        return this->WordsCount() * sizeof(UInt64_t);
    }

    UInt32_t DataVector::WordsCount() const{
        return DataVector::WordsCount(this->_count);
    }

    UInt32_t DataVector::WordsCount(const UInt32_t count){
        return (count + 63) / 64;
    }

    UInt32_t DataVector::BitSizeFromBool(const UInt32_t count){
        return (count * sizeof(bool) + 7) / 8;
    }

    DataVector* DataVector::FlatVector(
        const Memory::IAllocator* allocator,
        const DataType type,
        const UInt32_t count
    ){
        auto* dataVector = allocator->Allocate<DataVector>(type);
        dataVector->_count = count;
        dataVector->_kind  = DataVectorKind::Flat;
        dataVector->_dataEntrySize = VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<Int32_t>(type)];
        dataVector->_data  = static_cast<object_t*>(allocator->AllocateRaw(dataVector->_dataEntrySize * count));
        std::memset(dataVector->_data, 0, dataVector->_dataEntrySize * count);

        const auto validityWords = dataVector->WordsCount();
        dataVector->_validity = static_cast<UInt64_t*>(allocator->AllocateRaw(validityWords * sizeof(UInt64_t)));
        std::memset(dataVector->_validity, 0, validityWords * sizeof(UInt64_t));   // 0 = not-null default
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
        dataVector->_dataEntrySize = VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<Int32_t>(type)];
        dataVector->_data  = static_cast<object_t*>(allocator->AllocateRaw(dataVector->_dataEntrySize));
        std::memset(dataVector->_data, 0, dataVector->_dataEntrySize);

        dataVector->_validity = static_cast<UInt64_t*>(allocator->AllocateRaw(sizeof(UInt64_t)));
        dataVector->_validity[0] = isNull
            ? ~0ull
            : 0ull;
        return dataVector;
    }

    void DataVector::SetAllNull() const{
        std::memset(this->_validity, 0xFF, this->WordsCount() * sizeof(UInt64_t));
    }

    void DataVector::CopyValidity(const DataVector* other) const{
        std::memcpy(this->_validity, other->_validity, this->WordsCount() * sizeof(UInt64_t));
    }

    void DataVector::OrValidity(const DataVector* lhs, const DataVector* rhs) const{
        for (Int32_t i = 0; i < lhs->WordsCount(); ++i)
            this->_validity[i] |= lhs->_validity[i] | rhs->_validity[i];
    }

    void DataVector::ConvertToDictionary(const UInt32_t* selection){
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

    void DataChunk::AllocateColumns(const Memory::IAllocator* allocator, const Int32_t numberOfRows, const Int32_t numberOfColumns){
        this->_columns = static_cast<DataVector**>(
            allocator->AllocateRaw(sizeof(DataVector*)*numberOfColumns)
        );

        this->_numberOfRows = numberOfRows;
        this->_numberOfColumns = numberOfColumns;
    }

    void DataChunk::SetColumn(DataVector* columnData, const Int32_t columnIndex) const{
        this->_columns[columnIndex] = columnData;
    }

    Int32_t DataChunk::RowPhysicalIndex(const Int32_t rowLogicalIndex) const{
        return this->_selection == nullptr
            ? rowLogicalIndex
            : static_cast<Int32_t>(this->_selection[rowLogicalIndex]);
    }
}
