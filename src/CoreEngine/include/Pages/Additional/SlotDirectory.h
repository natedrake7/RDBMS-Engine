#pragma once
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

namespace Pages{
    struct SlotDirectory{
    private:
        UnsignedSmallInt flags_offset;
        UnsignedSmallInt _dataSize;

        UnsignedSmallInt _offset;
        UnsignedSmallInt _keySize;

        static constexpr UnsignedSmallInt OFFSET_MASK = 0x3FFF; // lower 14 bits
        static constexpr UnsignedSmallInt FLAGS_MASK  = 0xC000; // upper 2 bits

    public:
        enum Flag : UnsignedTinyInt {
            SLOT_EMPTY      = 0,
            SLOT_USED       = 1,
            SLOT_FORWARDED  = 2,
            SLOT_DEAD       = 3
        };

        static constexpr UnsignedTinyInt SIZE = 8;

        SlotDirectory();
        SlotDirectory(
            UnsignedSmallInt offset,
            UnsignedSmallInt dataOffset,
            UnsignedSmallInt dataSize,
            UnsignedSmallInt keySize,
            Flag flag
        );

        [[nodiscard]] UnsignedSmallInt AbsoluteDataOffset() const;

        [[nodiscard]] UnsignedSmallInt DataOffset() const;
        void SetDataOffset(UnsignedSmallInt otherOffset);

        [[nodiscard]] UnsignedSmallInt DataSize() const;
        void SetDataSize(UnsignedSmallInt otherSize);

        [[nodiscard]] UnsignedSmallInt Offset() const;
        void SetOffset(UnsignedSmallInt otherOffset);

        [[nodiscard]] UnsignedSmallInt KeySize() const;
        void SetKeySize(UnsignedSmallInt otherSize);

        [[nodiscard]] UnsignedSmallInt Size()const;

        [[nodiscard]] Flag GetFlag() const;
        void SetFlag(Flag otherFlag);

        [[nodiscard]] bool Empty() const;
        [[nodiscard]] bool Used() const;
        [[nodiscard]] bool ForwardPointer() const;
        [[nodiscard]] bool Dead() const;
        [[nodiscard]] bool Default() const;
    };

    struct SlotDirectoryDefragment{
        Int indexPosition;
        SlotDirectory slotDirectory;

        SlotDirectoryDefragment(
            const SlotDirectory slotDirectory,
            const Int indexPosition
        ){
            this->slotDirectory = slotDirectory;
            this->indexPosition = indexPosition;
        }

        static bool OrderAscendingByOffSet(const SlotDirectoryDefragment& lhs, const SlotDirectoryDefragment& rhs){
            return lhs.slotDirectory.DataOffset() < rhs.slotDirectory.DataOffset();
        }
    };
}
