#pragma once
#include <CoreEngine/DatabaseConstants.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>

namespace CoreEngine{
    struct SelectionVector{
        UInt32_t* selectedRids[Constants::MAX_QUERY_JOINS];

        // UnsignedTinyInt sourceMap[Constants::MAX_QUERY_JOINS];

        Int32_t selectedRidsCount;
        bool isIdentity;

        SelectionVector()
            :   selectedRids{nullptr},
                selectedRidsCount(0), isIdentity(false) {}

        void AllocateRids(const ::Memory::IAllocator* allocator, Int32_t index, Int32_t size);
    };

    enum class DataVectorKind : UInt8_t{
        Flat = 0,
        Constant = 1,
        Dictionary = 2
    };

    struct DataVector{
        object_t* _data;
        UInt64_t* _validity;
        const UInt32_t* _selection;

        UInt32_t _count;

        DataType _type;
        DataVectorKind _kind;

        UInt16_t _dataEntrySize;

        explicit DataVector(DataType type);

        [[nodiscard]] inline object_t* SlotAt(const UInt32_t index) const{
            return this->_data + this->_dataEntrySize * index;
        }

        void SetNullValue(UInt32_t index, bool value) const;

        [[nodiscard]] UInt32_t PhysicalIndex(UInt32_t logicalIndex) const;
        [[nodiscard]] UInt32_t DictionaryIndex(UInt32_t logicalIndex) const;
        [[nodiscard]] bool GetNullValue(const UInt32_t index) const{
            return EngineBitmap::GetBitmapBit(this->_validity, index);
        }

        [[nodiscard]] UInt32_t ValidityBytes()const;
        [[nodiscard]] UInt32_t WordsCount()const;


        [[nodiscard]] static UInt32_t WordsCount(UInt32_t count);
        [[nodiscard]] static UInt32_t BitSizeFromBool(UInt32_t count);

        template<typename T>
        T* SlotAt(const UInt32_t physicalIndex){
            return reinterpret_cast<T*>(this->SlotAt(physicalIndex));
        }

        template<typename T>
        const T* SlotAt(const UInt32_t physicalIndex) const{
            return reinterpret_cast<const T*>(this->SlotAt(physicalIndex));
        }

        template<typename T>
        T* DataAs(){
            return reinterpret_cast<T*>(this->_data);
        }

        [[nodiscard]] UInt64_t* DataAsWords() const{
            return reinterpret_cast<UInt64_t*>(this->_data);
        }

        template<typename T>
        const T* DataAs()const{
            return reinterpret_cast<const T*>(this->_data);
        }

        static DataVector* FlatVector(
            const ::Memory::IAllocator* allocator,
            DataType type,
            UInt32_t count
        );

        static DataVector* ConstantVector(
            const ::Memory::IAllocator* allocator,
            bool isNull,
            DataType type
        );

        template<typename T>
        static inline T& FlatAccess(T* data, const Int32_t index){
            return data[index];
        }

        template<typename T>
        static inline T& ConstantAccess(T* data){
            return data[0];
        }

        template<typename T>
        static inline T& DictionaryAccess(DataVector* vector, const Int32_t index){
            return vector->SlotAt<T>(vector->PhysicalIndex(index));
        }

        void SetAllNull() const;
        void CopyValidity(const DataVector* other) const;
        void OrValidity(const DataVector* lhs, const DataVector* rhs) const;

        void ConvertToDictionary(const UInt32_t* selection);

        [[nodiscard]] bool IsConstant()const;
        [[nodiscard]] bool IsFlat()const;
        [[nodiscard]] bool IsDictionary()const;
    };

    struct DataChunk{
        DataVector** _columns;

        UInt32_t* _selection;

        Int32_t _numberOfColumns;
        Int32_t _numberOfRows;

        DataChunk();
        DataChunk(const DataChunk& other) = delete;
        DataChunk& operator=(const DataChunk& other) = delete;

        DataChunk(DataChunk&& other) noexcept;
        DataChunk& operator=(DataChunk&& other) noexcept;

        void AllocateColumns(const ::Memory::IAllocator* allocator, Int32_t numberOfRows, Int32_t numberOfColumns);
        void SetColumn(DataVector* columnData, Int32_t columnIndex) const;
        [[nodiscard]] Int32_t RowPhysicalIndex(Int32_t rowLogicalIndex) const;
    };
}
