#pragma once
#include "../DatabaseConstants.h"

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

        Int _count;

        DataType _type;
        DataVectorKind _kind;

        UnsignedSmallInt _dataEntrySize;

        explicit DataVector(DataType type);

        [[nodiscard]] object_t* SlotAt(Int index) const;
        void SetNullValue(Int index, bool value) const;
       [[nodiscard]] bool GetNullValue(Int index) const;

        static DataVector* FlatVector(
            const ::Memory::IAllocator* allocator,
            DataType type,
            Int count
        );
    };

    struct DataChunk{
        DataVector** _columns;

        const UnsignedInt* _selection;

        Int _numberOfColumns;
        Int _numberOfRows;

        DataChunk();
        DataChunk(const DataChunk& other) = delete;
        DataChunk& operator=(const DataChunk& other) = delete;

        DataChunk(DataChunk&& other) noexcept;
        DataChunk& operator=(DataChunk&& other) noexcept;

        void AllocateColumns(const ::Memory::IAllocator* allocator, Int numberOfColumns);
        void SetColumn(DataVector* columnData, Int columnIndex) const;
        [[nodiscard]] Int RowPhysicalIndex(Int rowLogicalIndex) const;
    };
}
