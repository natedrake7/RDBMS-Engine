#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace CoreEngine{
    struct SelectionVector{
        UnsignedInt* selectedRids[Constants::MAX_QUERY_JOINS];

        // UnsignedTinyInt sourceMap[Constants::MAX_QUERY_JOINS];

        Int selectedRidsCount;
        bool isIdentity;

        SelectionVector()
            :   selectedRids{nullptr},
                selectedRidsCount(0), isIdentity(false) {}

        void AllocateRids(const ::Memory::IAllocator* allocator, Int index, Int size);
    };

    enum class DataVectorKind : UnsignedTinyInt{
        Flat = 0,
        Constant = 1,
        Dictionary = 2
    };

    struct DataVector{
        object_t* _data;
        UnsignedBigInt* _validity;
        const UnsignedInt* _selection;

        Int _count;

        DataType _type;
        DataVectorKind _kind;

        UnsignedSmallInt _dataEntrySize;

        explicit DataVector(DataType type);

        [[nodiscard]] object_t* SlotAt(Int index) const;
        void SetNullValue(Int index, bool value) const;

        [[nodiscard]] UnsignedInt PhysicalIndex(Int logicalIndex) const;
        [[nodiscard]] Int DictionaryIndex(Int logicalIndex) const;
        [[nodiscard]] bool GetNullValue(Int index) const;

        template<typename T>
        T* SlotAt(const Int physicalIndex){
            return reinterpret_cast<T*>(this->SlotAt(physicalIndex));
        }

        template<typename T>
        const T* SlotAt(const Int physicalIndex) const{
            return reinterpret_cast<const T*>(this->SlotAt(physicalIndex));
        }

        template<typename T>
        T* DataAs(){
            return reinterpret_cast<T*>(this->_data);
        }

        template<typename T>
        const T* DataAs()const{
            return reinterpret_cast<const T*>(this->_data);
        }

        static DataVector* FlatVector(
            const ::Memory::IAllocator* allocator,
            DataType type,
            Int count
        );

        static DataVector* ConstantVector(
            const ::Memory::IAllocator* allocator,
            bool isNull,
            DataType type
        );

        template<typename T>
        static inline T& FlatAccess(T* data, const Int index){
            return data[index];
        }

        template<typename T>
        static inline T& ConstantAccess(T* data){
            return data[0];
        }

        template<typename T>
        static inline T& DictionaryAccess(DataVector* vector, const Int index){
            return vector->SlotAt<T>(vector->PhysicalIndex(index));
        }

        [[nodiscard]] Int ValidityWords()const;
        void SetAllNull() const;
        void CopyValidity(const DataVector* other) const;
        void OrValidity(const DataVector* lhs, const DataVector* rhs) const;

        void ConvertToDictionary(const UnsignedInt* selection);

        [[nodiscard]] bool IsConstant()const;
        [[nodiscard]] bool IsFlat()const;
        [[nodiscard]] bool IsDictionary()const;
    };

    struct DataChunk{
        DataVector** _columns;

        UnsignedInt* _selection;

        Int _numberOfColumns;
        Int _numberOfRows;

        DataChunk();
        DataChunk(const DataChunk& other) = delete;
        DataChunk& operator=(const DataChunk& other) = delete;

        DataChunk(DataChunk&& other) noexcept;
        DataChunk& operator=(DataChunk&& other) noexcept;

        void AllocateColumns(const ::Memory::IAllocator* allocator, Int numberOfRows, Int numberOfColumns);
        void SetColumn(DataVector* columnData, Int columnIndex) const;
        [[nodiscard]] Int RowPhysicalIndex(Int rowLogicalIndex) const;
    };
}
