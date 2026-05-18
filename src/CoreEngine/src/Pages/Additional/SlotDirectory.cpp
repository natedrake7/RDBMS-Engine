#include "../../../include/Pages/Additional/SlotDirectory.h"

namespace Pages{
    SlotDirectory::SlotDirectory()
        :   flags_offset(0), _dataSize(0),
            _offset(0), _keySize(0){}

    SlotDirectory::SlotDirectory(
        const UnsignedSmallInt offset,
        const UnsignedSmallInt dataOffset,
        const UnsignedSmallInt dataSize,
        const UnsignedSmallInt keySize,
        const Flag flag
    ){
        this->_offset = offset;
        this->_dataSize = dataSize;
        this->_keySize = keySize;
        this->SetDataOffset(dataOffset);
        this->SetFlag(flag);
    }

    UnsignedSmallInt SlotDirectory::AbsoluteDataOffset() const{
        return this->_offset + this->DataOffset();
    }

    void SlotDirectory::SetDataOffset(const UnsignedSmallInt otherOffset){
        this->flags_offset = (this->flags_offset & FLAGS_MASK) | (otherOffset & OFFSET_MASK);
    }

    UnsignedSmallInt SlotDirectory::DataOffset() const{
        return this->flags_offset & OFFSET_MASK;
    }

    void SlotDirectory::SetDataSize(const UnsignedSmallInt otherSize){
        this->_dataSize = otherSize;
    }

    UnsignedSmallInt SlotDirectory::Offset() const{
        return this->_offset;
    }

    void SlotDirectory::SetOffset(const UnsignedSmallInt otherOffset){
        this->_offset = otherOffset;
    }

    UnsignedSmallInt SlotDirectory::KeySize() const{
        return this->_keySize;
    }

    void SlotDirectory::SetKeySize(const UnsignedSmallInt otherSize){
        this->_keySize = otherSize;
    }

    UnsignedSmallInt SlotDirectory::Size() const{
        return this->_dataSize + this->_keySize;
    }

    UnsignedSmallInt SlotDirectory::DataSize() const{
        return this->_dataSize;
    }

    void SlotDirectory::SetFlag(const Flag otherFlag){
        this->flags_offset = (this->flags_offset & OFFSET_MASK) | ((otherFlag << 14) & FLAGS_MASK);
    }

    SlotDirectory::Flag SlotDirectory::GetFlag() const{
        return static_cast<Flag>((this->flags_offset & FLAGS_MASK) >> 14);
    }

    bool SlotDirectory::Empty() const { return this->GetFlag() == SLOT_EMPTY; }
    bool SlotDirectory::Used() const { return this->GetFlag() == SLOT_USED; }
    bool SlotDirectory::ForwardPointer() const { return this->GetFlag() == SLOT_FORWARDED; }
    bool SlotDirectory::Dead() const { return this->GetFlag() == SLOT_DEAD; }
    bool SlotDirectory::Default() const{
        return this->flags_offset == 0 && this->_dataSize == 0;
    }
}