#pragma once
#include <Systemic/DataTypes/DataTypes.h>

#include "CoreEngine/Pages/PageLayout.h"

namespace CoreEngine::Pages{
    struct SlotDirectory{
    private:
        UInt16_t flags_offset;
        UInt16_t _dataSize;

        UInt16_t _offset;
        UInt16_t _keySize;

        static constexpr UInt16_t OFFSET_MASK = 0x3FFF; // lower 14 bits
        static constexpr UInt16_t FLAGS_MASK  = 0xC000; // upper 2 bits

    public:
        enum Flag : UInt8_t {
            SLOT_EMPTY      = 0,
            SLOT_USED       = 1,
            SLOT_FORWARDED  = 2,
            SLOT_DEAD       = 3
        };

        SlotDirectory()
        :   flags_offset(0), _dataSize(0),
            _offset(0), _keySize(0){}

        explicit SlotDirectory(
            const UInt16_t offset,
            const UInt16_t dataOffset,
            const UInt16_t dataSize,
            const UInt16_t keySize,
            const Flag flag
        ):  flags_offset(0), _dataSize(dataSize),
            _offset(offset), _keySize(keySize){
            this->SetDataOffset(dataOffset);
            this->SetFlag(flag);
        }

        static SlotDirectory DeadSlot(){
            return SlotDirectory(0, 0, 0, 0, SLOT_DEAD);
        }

        [[nodiscard]] UInt16_t AbsoluteDataOffset() const{
            return this->_offset + this->DataOffset();
        }

        [[nodiscard]] UInt16_t AbsoluteKeyOffset() const{
            return this->_offset;
        }

        void SetDataOffset(const UInt16_t otherOffset){
            this->flags_offset = (this->flags_offset & FLAGS_MASK) | (otherOffset & OFFSET_MASK);
        }

        [[nodiscard]] UInt16_t DataOffset() const{
            return this->flags_offset & OFFSET_MASK;
        }

        void SetDataSize(const UInt16_t otherSize){
            this->_dataSize = otherSize;
        }

        [[nodiscard]] UInt16_t Offset() const{
            return this->_offset;
        }

        void SetOffset(const UInt16_t otherOffset){
            this->_offset = otherOffset;
        }

        [[nodiscard]] UInt16_t KeySize() const{
            return this->_keySize;
        }

        void SetKeySize(const UInt16_t otherSize){
            this->_keySize = otherSize;
        }

        [[nodiscard]] UInt16_t Size() const{
            return this->_dataSize + this->_keySize;
        }

        [[nodiscard]] UInt16_t DataSize() const{
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
        sizeof(SlotDirectory) == PageLayout::SLOT_DIRECTORY_SIZE,
        "SlotDirectory size mismatch"
    );

    struct SlotDirectoryDefragment{
        SlotDirectory _slotDirectory;
        Int32_t _indexPosition;

       explicit SlotDirectoryDefragment(
            const SlotDirectory slotDirectory,
            const Int32_t indexPosition
        ):  _slotDirectory(slotDirectory),
            _indexPosition(indexPosition){}
    };
}
