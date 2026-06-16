#include "../../include/Managers/GlobalMemoryManager.h"

#include "DatabaseConstants.h"
#include "Memory/Functions.h"

namespace CoreEngine{
    GlobalMemoryManager::GlobalMemoryManager(){
        this->_dbCapacity = 0;
        this->_bufferPoolCapacity = 0;
        this->_executionCapacity = 0;
        this->_executionUsed = 0;
        this->_miscCapacity = 0;
        this->_miscUsed = 0;
    }

    void GlobalMemoryManager::Initialize(const ::Memory::OSMemoryInfo& memoryInfo){
        this->_dbCapacity =  (memoryInfo.availablePhysicalBytes * DB_RATIO_NUMERATOR) / DB_RATIO_DENOMINATOR;
        this->_bufferPoolCapacity = (this->_dbCapacity * BUFFER_POOL_RATIO_NUMERATOR) / BUFFER_POOL_RATIO_DENOMINATOR;
        this->_executionCapacity = (this->_dbCapacity * EXECUTION_RATIO_NUMERATOR) / EXECUTION_RATIO_DENOMINATOR;
        this->_miscCapacity = (this->_dbCapacity * MISC_RATIO_NUMERATOR) / MISC_RATIO_DENOMINATOR;
    }

    GlobalMemoryManager& GlobalMemoryManager::Get(){
        static GlobalMemoryManager _instance;
        return _instance;
    }

    bool GlobalMemoryManager::TryReserveForExecution(const UnsignedBigInt size){
        auto current = this->_executionUsed.load(std::memory_order_relaxed);
        do{
            if (current + size > this->_executionCapacity)
                return false;
        } while (!this->_executionUsed.compare_exchange_weak(
                    current, current + size,
                    std::memory_order_relaxed, std::memory_order_relaxed));

        return true;
    }

    void GlobalMemoryManager::ReleaseExecutionReservation(const UnsignedBigInt size){
        this->_executionUsed.fetch_sub(size, std::memory_order_relaxed);
    }

    bool GlobalMemoryManager::TryReserveForMisc(const UnsignedBigInt size){
        auto current = this->_miscUsed.load(std::memory_order_relaxed);
        do{
            if (current + size > this->_miscCapacity)
                return false;
        } while (!this->_miscUsed.compare_exchange_weak(
                    current, current + size,
                    std::memory_order_relaxed, std::memory_order_relaxed));

        return true;
    }

    void GlobalMemoryManager::ReleaseMiscReservation(const UnsignedBigInt size){
        this->_miscUsed.fetch_sub(size, std::memory_order_relaxed);
    }

    UnsignedBigInt GlobalMemoryManager::GetDbCapacity() const{ return this->_dbCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetBufferPoolCapacity() const{ return this->_bufferPoolCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetMiscCapacity() const{ return this->_miscCapacity; }

    UnsignedBigInt GlobalMemoryManager::GetMiscReservation() const{
        return this->_miscUsed.load(std::memory_order_relaxed);
    }

    UnsignedBigInt GlobalMemoryManager::GetExecutionCapacity() const{ return this->_executionCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetExecutionReservation() const{
        return this->_executionUsed.load(std::memory_order_relaxed);
    }

    void GlobalMemoryManager::Log(std::ostream& os, const ::Memory::MemoryLogLevel level) const{
        const UnsignedBigInt executionUsed = this->_executionUsed.load(std::memory_order_relaxed);
        const UnsignedBigInt miscUsed = this->_miscUsed.load(std::memory_order_relaxed);

        switch (level){
            case ::Memory::MemoryLogLevel::Bytes:{
                os << "Total Database Memory: " << this->_dbCapacity << " Bytes\n";
                os << "Total Buffer Pool Memory: " << this->_bufferPoolCapacity << " Bytes\n";
                os << "Total Execution Pipeline Memory: " << this->_executionCapacity << " Bytes\n";
                os << "Total Execution Pipeline Memory Used: " << executionUsed << " Bytes\n";
                os << "Total Miscellaneous Memory: " << this->_miscCapacity << " Bytes\n";
                os << "Total Miscellaneous Memory Used: " << miscUsed << " Bytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::KiloBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::MegaBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::GigaBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                break;
            }
            default: break;
        }
    }
}
