#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory{
    enum class MemoryLogLevel : UnsignedTinyInt {
        Bytes = 0,
        KiloBytes = 1,
        MegaBytes = 2,
        GigaBytes = 3
    };

    constexpr Int BYTES_TO_KB = 1024;
    constexpr Int BYTES_TO_MB = 1024 * 1024;
    constexpr Int BYTES_TO_GB = 1024 * 1024 * 1024;

    struct OSMemoryInfo{
        UnsignedBigInt totalPhysicalBytes;
        UnsignedBigInt availablePhysicalBytes;
        UnsignedBigInt totalVirtualBytes;
        UnsignedBigInt availableVirtualBytes;

        OSMemoryInfo();
        void Log(std::ostream& os, MemoryLogLevel level) const;
    };

    OSMemoryInfo GetOSMemoryInfo();
}
