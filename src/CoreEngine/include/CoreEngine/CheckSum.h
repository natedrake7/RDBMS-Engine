#pragma once
#include <CoreEngine/Pages/PageView.h>
#include <Systemic/CheckSum.h>

namespace CoreEngine{
    [[nodiscard]] inline bool VerifyFrameCheckSumNoLock(Pages::PageHeader* header, const object_t* data){
        const auto stored = header->_checkSum;
        header->_checkSum = 0;
        const auto actual = CheckSum::Crc32Fast<StorageLayout::PAGE_SIZE>(data);
        header->_checkSum = stored;
        return stored == actual;
    }
}
