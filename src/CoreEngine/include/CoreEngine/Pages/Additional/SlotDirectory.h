#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine::Pages{
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

        inline static constexpr UnsignedTinyInt SIZE = 4 * sizeof(UnsignedSmallInt);

        SlotDirectory()
        :   flags_offset(0), _dataSize(0),
            _offset(0), _keySize(0){}

        explicit SlotDirectory(
            const UnsignedSmallInt offset,
            const UnsignedSmallInt dataOffset,
            const UnsignedSmallInt dataSize,
            const UnsignedSmallInt keySize,
            const Flag flag
        ):  flags_offset(0), _dataSize(dataSize),
            _offset(offset), _keySize(keySize){
            this->SetDataOffset(dataOffset);
            this->SetFlag(flag);
        }

        static SlotDirectory DeadSlot(){
            return SlotDirectory(0, 0, 0, 0, SLOT_DEAD);
        }

        [[nodiscard]] UnsignedSmallInt AbsoluteDataOffset() const{
            return this->_offset + this->DataOffset();
        }

        [[nodiscard]] UnsignedSmallInt AbsoluteKeyOffset() const{
            return this->_offset;
        }

        void SetDataOffset(const UnsignedSmallInt otherOffset){
            this->flags_offset = (this->flags_offset & FLAGS_MASK) | (otherOffset & OFFSET_MASK);
        }

        [[nodiscard]] UnsignedSmallInt DataOffset() const{
            return this->flags_offset & OFFSET_MASK;
        }

        void SetDataSize(const UnsignedSmallInt otherSize){
            this->_dataSize = otherSize;
        }

        [[nodiscard]] UnsignedSmallInt Offset() const{
            return this->_offset;
        }

        void SetOffset(const UnsignedSmallInt otherOffset){
            this->_offset = otherOffset;
        }

        [[nodiscard]] UnsignedSmallInt KeySize() const{
            return this->_keySize;
        }

        void SetKeySize(const UnsignedSmallInt otherSize){
            this->_keySize = otherSize;
        }

        [[nodiscard]] UnsignedSmallInt Size() const{
            return this->_dataSize + this->_keySize;
        }

        [[nodiscard]] UnsignedSmallInt DataSize() const{
            return this->_dataSize;
        }

        void SetFlag(const Flag otherFlag){
            this->flags_offset = (this->flags_offset & OFFSET_MASK) | ((otherFlag << 14) & FLAGS_MASK);
        }

        [[nodiscard]] Flag GetFlag() const{
            return static_cast<Flag>((this->flags_offset & FLAGS_MASK) >> 14);
        }

        [[nodiscard]] bool Empty() const { return this->GetFlag() == SLOT_EMPTY; }
        [[nodiscard]] bool Used() const { return this->GetFlag() == SLOT_USED; }
        [[nodiscard]] bool ForwardPointer() const { return this->GetFlag() == SLOT_FORWARDED; }
        [[nodiscard]] bool Dead() const { return this->GetFlag() == SLOT_DEAD; }
        [[nodiscard]] bool Default() const{
            return this->flags_offset == 0 && this->_dataSize == 0;
        }
    };

    static_assert(
        sizeof(SlotDirectory) == SlotDirectory::SIZE,
        "SlotDirectory size mismatch"
    );

    struct SlotDirectoryDefragment{
        SlotDirectory _slotDirectory;
        Int _indexPosition;

       explicit SlotDirectoryDefragment(
            const SlotDirectory slotDirectory,
            const Int indexPosition
        ):  _slotDirectory(slotDirectory),
            _indexPosition(indexPosition){}
    };
}
