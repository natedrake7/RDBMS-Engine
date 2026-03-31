#include "../../include/Managers/GlobalMemoryManager.h"

#include "DatabaseConstants.h"
#include "Guards/ReaderGuard.h"
#include "Guards/WriterGuard.h"
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
        MultiThreading::WriterGuard lock(&this->_executionPoolMutex);

        if (this->_executionUsed + size > this->_executionCapacity)
            return false;

        this->_executionUsed += size;
        return true;
    }

    void GlobalMemoryManager::ReleaseExecutionReservation(const UnsignedBigInt size){
        MultiThreading::WriterGuard lock(&this->_executionPoolMutex);
        this->_executionUsed -= size;
    }

    bool GlobalMemoryManager::TryReserveForMisc(const UnsignedBigInt size){
        MultiThreading::WriterGuard lock(&this->_miscPoolMutex);

        if (this->_miscUsed + size > this->_miscCapacity)
            return false;

        this->_miscUsed += size;
        return true;
    }

    void GlobalMemoryManager::ReleaseMiscReservation(const UnsignedBigInt size){
        MultiThreading::WriterGuard lock(&this->_miscPoolMutex);
        this->_miscUsed -= size;
    }

    UnsignedBigInt GlobalMemoryManager::GetDbCapacity() const{ return this->_dbCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetBufferPoolCapacity() const{ return this->_bufferPoolCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetMiscCapacity() const{ return this->_miscCapacity; }

    UnsignedBigInt GlobalMemoryManager::GetMiscReservation() const{
        MultiThreading::ReaderGuard lock(&this->_miscPoolMutex);
        return this->_miscUsed;
    }

    UnsignedBigInt GlobalMemoryManager::GetExecutionCapacity() const{ return this->_executionCapacity; }
    UnsignedBigInt GlobalMemoryManager::GetExecutionReservation() const{
        MultiThreading::ReaderGuard lock(&this->_executionPoolMutex);
        return this->_executionUsed;
    }

    void GlobalMemoryManager::Log(std::ostream& os, const ::Memory::MemoryLogLevel level) const{
        switch (level){
            case ::Memory::MemoryLogLevel::Bytes:{
                os << "Total Database Memory: " << this->_dbCapacity << " Bytes\n";
                os << "Total Buffer Pool Memory: " << this->_bufferPoolCapacity << " Bytes\n";
                os << "Total Execution Pipeline Memory: " << this->_executionCapacity << " Bytes\n";
                os << "Total Execution Pipeline Memory Used: " << this->_executionUsed << " Bytes\n";
                os << "Total Miscellaneous Memory: " << this->_miscCapacity << " Bytes\n";
                os << "Total Miscellaneous Memory Used: " << this->_miscUsed << " Bytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::KiloBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (this->_executionUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                os << "Total Miscellaneous Memory Used: " << (this->_miscUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::MegaBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (this->_executionUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                os << "Total Miscellaneous Memory Used: " << (this->_miscUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
                break;
            }
            case ::Memory::MemoryLogLevel::GigaBytes:{
                os << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Execution Pipeline Memory Used: " << (this->_executionUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                os << "Total Miscellaneous Memory Used: " << (this->_miscUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
                break;
            }
            default: break;
        }
    }
}
