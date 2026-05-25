#include "../../include/Vectorization/Vectorization.h"

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->selectedRids[index] = static_cast<UnsignedInt*>(allocator->AllocateRaw(size * sizeof(UnsignedInt)));
        this->isIdentity = false;
    }

    void SelectionVector::AllocateNullMask(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->nullMask[index] = static_cast<UnsignedTinyInt*>(allocator->AllocateRaw(size));
        this->isIdentity = false;
    }

    DataVector::DataVector()
        :   _data{nullptr}, _validity{nullptr},
            _count(0), _type(DataType::Null),
            _kind(DataVectorKind::Flat) {}

    // void VectorBatch::AllocateColumns(const Memory::IAllocator* allocator, const Int numberOfColumns){
    //     this->_columns = static_cast<CoreEngine::DataVector**>(allocator->AllocateRaw(numberOfColumns * sizeof(CoreEngine::DataVector*)));
    //     this->_numberOfColumns = numberOfColumns;
    //     this->_numberOfRows = 0;
    // }
    //
    // void VectorBatch::SetColumn(CoreEngine::DataVector* columnData, const Int column) const{
    //     this->_columns[column] = columnData;
    // }
}
