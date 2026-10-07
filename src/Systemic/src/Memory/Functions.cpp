#include <Systemic/Memory/Functions.h>
#include <ostream>

#ifdef _WIN32
    #include <windows.h>
#else
    #include <sys/sysinfo.h>
#endif

namespace Memory{
    OSMemoryInfo::OSMemoryInfo(){
        this->totalPhysicalBytes = 0;
        this->availablePhysicalBytes = 0;
        this->totalVirtualBytes = 0;
        this->availableVirtualBytes = 0;
    }

    void OSMemoryInfo::Log(std::ostream& os, const MemoryLogLevel level) const{
        // The enumerator names double as unit labels ("Bytes", "KiloBytes", ...)
        // and every level is one more factor of 1024.
        const auto unit = Reflection::EnumIdentifier(level);
        if (unit.empty())
            return;

        const auto shift = 10 * static_cast<UnsignedInt>(level);
        const auto print = [&](const char* label, const UnsignedBigInt bytes){
            os << label << (bytes >> shift) << ' ' << unit << '\n';
        };

        print("Total Physical Memory: ", this->totalPhysicalBytes);
        print("Available Physical Memory: ", this->availablePhysicalBytes);
        print("Total Virtual Memory: ", this->totalVirtualBytes);
        print("Available Virtual Memory: ", this->availableVirtualBytes);
    }

    OSMemoryInfo GetOSMemoryInfo()
    {
        OSMemoryInfo info;
#ifdef _WIN32
        MEMORYSTATUSEX memStatus;
        memStatus.dwLength = sizeof(memStatus);
        if (GlobalMemoryStatusEx(&memStatus)){
            info.totalPhysicalBytes = memStatus.ullTotalPhys;
            info.availablePhysicalBytes = memStatus.ullAvailPhys;
            info.totalVirtualBytes = memStatus.ullTotalVirtual;
            info.availableVirtualBytes = memStatus.ullAvailVirtual;
            return info;
        }
#else
        struct sysinfo s = {};
        if (sysinfo(&s) == 0) {
            const auto unit = s.mem_unit
                ? s.mem_unit
                : 1ULL;
            info.totalPhysicalBytes = s.totalram * unit;
            info.availablePhysicalBytes = s.freeram * unit;
            info.totalVirtualBytes = s.totalram + s.totalswap * unit;
            info.availableVirtualBytes = s.freeram + s.freeswap * unit;
        }
#endif
        return info;
    }
}
