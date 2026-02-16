#include "../../include/Memory/Functions.h"
#include <ostream>

#ifdef _WIN32
    #include <windows.h>
#else

#endif

namespace Memory{
    OSMemoryInfo::OSMemoryInfo(){
        this->totalPhysicalBytes = 0;
        this->availablePhysicalBytes = 0;
        this->totalVirtualBytes = 0;
        this->availableVirtualBytes = 0;
    }

    std::ostream& OSMemoryInfo::Log(std::ostream& os, const MemoryLogLevel level) const{
        switch (level){
            case MemoryLogLevel::Bytes:{
                os << "Total Physical Memory: " << (this->totalPhysicalBytes) << " Bytes\n";
                os << "Available Physical Memory: " << (this->availablePhysicalBytes) << " Bytes\n";
                os << "Total Virtual Memory: " << (this->totalVirtualBytes) << " Bytes\n";
                os << "Available Virtual Memory: " << (this->availableVirtualBytes) << " Bytes\n";
                break;
            }
            case MemoryLogLevel::KiloBytes:{
                constexpr Int BYTES_TO_KB = 1024;
                os << "Total Physical Memory: " << (this->totalPhysicalBytes / BYTES_TO_KB) << " KiloBytes\n";
                os << "Available Physical Memory: " << (this->availablePhysicalBytes / BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Virtual Memory: " << (this->totalVirtualBytes / BYTES_TO_KB) << " KiloBytes\n";
                os << "Available Virtual Memory: " << (this->availableVirtualBytes / BYTES_TO_KB) << " KiloBytes\n";
                break;
            }
            case MemoryLogLevel::MegaBytes:{
                constexpr Int BYTES_TO_MB = 1024 * 1024;
                os << "Total Physical Memory: " << (this->totalPhysicalBytes / BYTES_TO_MB) << " MegaBytes\n";
                os << "Available Physical Memory: " << (this->availablePhysicalBytes / BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Virtual Memory: " << (this->totalVirtualBytes / BYTES_TO_MB) << " MegaBytes\n";
                os << "Available Virtual Memory: " << (this->availableVirtualBytes / BYTES_TO_MB) << " MegaBytes\n";
                break;
            }
            case MemoryLogLevel::GigaBytes:{
                constexpr Int BYTES_TO_GB = 1024 * 1024 * 1024;
                os << "Total Physical Memory: " << (this->totalPhysicalBytes / BYTES_TO_GB) << " GigaBytes\n";
                os << "Available Physical Memory: " << (this->availablePhysicalBytes / BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Virtual Memory: " << (this->totalVirtualBytes / BYTES_TO_GB) << " GigaBytes\n";
                os << "Available Virtual Memory: " << (this->availableVirtualBytes / BYTES_TO_GB) << " GigaBytes\n";
                break;
            }
            default: break;
        }

        return os;
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

#endif

        return info;
    }
}
