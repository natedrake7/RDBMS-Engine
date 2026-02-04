#include "../../../include/Pages/Additional/SlotDirectory.h"

namespace Pages{
    SlotDirectory::SlotDirectory(){
        this->flags_offset = 0;
        this->size = 0;
    }

    SlotDirectory::SlotDirectory(
        const UnsignedSmallInt offset,
        const UnsignedSmallInt size,
        const Flag flag
    ){
        this->SetOffset(offset);
        this->SetSize(size);
        this->SetFlag(flag);
    }

    void SlotDirectory::SetOffset(const UnsignedSmallInt otherOffset){
        this->flags_offset = (this->flags_offset & FLAGS_MASK) | (otherOffset & OFFSET_MASK);
    }

    UnsignedSmallInt SlotDirectory::GetOffset() const{
        return this->flags_offset & OFFSET_MASK;
    }

    void SlotDirectory::SetSize(const UnsignedSmallInt otherSize){
        this->size = otherSize;
    }

    UnsignedSmallInt SlotDirectory::GetSize() const{
        return this->size;
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
        return this->flags_offset == 0 && this->size == 0;
    }
}