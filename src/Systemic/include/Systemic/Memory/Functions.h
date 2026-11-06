#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <ostream>

namespace Memory{
    enum class MemoryLogLevel : UInt8_t {
        Bytes = 0,
        KiloBytes = 1,
        MegaBytes = 2,
        GigaBytes = 3
    };

    inline constexpr Int32_t BYTES_TO_KB = 1024;
    inline constexpr Int32_t BYTES_TO_MB = 1024 * 1024;
    inline constexpr Int32_t BYTES_TO_GB = 1024 * 1024 * 1024;

    struct OSMemoryInfo{
        UInt64_t totalPhysicalBytes;
        UInt64_t availablePhysicalBytes;
        UInt64_t totalVirtualBytes;
        UInt64_t availableVirtualBytes;

        OSMemoryInfo();
        void Log(std::ostream& os, MemoryLogLevel level) const;
    };

    OSMemoryInfo GetOSMemoryInfo();
}
