#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory{
    enum class MemoryLogLevel : UnsignedTinyInt {
        Bytes = 0,
        KiloBytes = 1,
        MegaBytes = 2,
        GigaBytes = 3
    };

    struct OSMemoryInfo{
        BigInt totalPhysicalBytes;
        BigInt availablePhysicalBytes;
        BigInt totalVirtualBytes;
        BigInt availableVirtualBytes;

        OSMemoryInfo();
        std::ostream& Log(std::ostream& os, MemoryLogLevel level) const;
    };

    OSMemoryInfo GetOSMemoryInfo();
}
