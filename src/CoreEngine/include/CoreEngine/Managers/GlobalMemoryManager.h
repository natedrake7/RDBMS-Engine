#pragma once
#include <atomic>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Memory/Functions.h>

namespace Memory{
    struct OSMemoryInfo;
}

namespace CoreEngine{
    class GlobalMemoryManager{
        UInt64_t _dbCapacity;
        UInt64_t _bufferPoolCapacity;
        UInt64_t _executionCapacity;
        UInt64_t _miscCapacity;

        std::atomic<UInt64_t> _executionUsed;
        std::atomic<UInt64_t> _miscUsed;

        static constexpr Int32_t DB_RATIO_NUMERATOR = 60;
        static constexpr Int32_t DB_RATIO_DENOMINATOR = 100;

        static constexpr Int32_t BUFFER_POOL_RATIO_NUMERATOR = 50;
        static constexpr Int32_t BUFFER_POOL_RATIO_DENOMINATOR = 100;

        static constexpr Int32_t EXECUTION_RATIO_NUMERATOR = 40;
        static constexpr Int32_t EXECUTION_RATIO_DENOMINATOR = 100;

        static constexpr Int32_t MISC_RATIO_NUMERATOR = 10;
        static constexpr Int32_t MISC_RATIO_DENOMINATOR = 100;

        GlobalMemoryManager();
        public:
            GlobalMemoryManager(const GlobalMemoryManager&) = delete;
            GlobalMemoryManager& operator=(const GlobalMemoryManager&) = delete;
            GlobalMemoryManager(GlobalMemoryManager&&) = delete;
            GlobalMemoryManager& operator=(GlobalMemoryManager&&) = delete;

            void Initialize(const ::Memory::OSMemoryInfo& memoryInfo);
            static GlobalMemoryManager& Get();

            [[nodiscard]] bool TryReserveForExecution(UInt64_t size);
            void ReleaseExecutionReservation(UInt64_t size);

            [[nodiscard]] bool TryReserveForMisc(UInt64_t size);
            void ReleaseMiscReservation(UInt64_t size);

            [[nodiscard]] UInt64_t GetDbCapacity()const;
            [[nodiscard]] UInt64_t GetBufferPoolCapacity()const;
            [[nodiscard]] UInt64_t GetMiscCapacity()const;
            [[nodiscard]] UInt64_t GetMiscReservation()const;
            [[nodiscard]] UInt64_t GetExecutionCapacity()const;
            [[nodiscard]] UInt64_t GetExecutionReservation()const;

            template<::Memory::MemoryLogLevel LEVEL>
            void Log(std::ostream& os)const;

            void Log(std::ostream& os, ::Memory::MemoryLogLevel level)const;
    };

    template <::Memory::MemoryLogLevel LEVEL>
    void GlobalMemoryManager::Log(std::ostream& os) const{
        const auto executionUsed = this->_executionUsed.load(std::memory_order_relaxed);
        const auto miscUsed = this->_miscUsed.load(std::memory_order_relaxed);

        if constexpr(LEVEL == ::Memory::MemoryLogLevel::Bytes){
            os  << "Total Database Memory: " << this->_dbCapacity << " Bytes\n"
                << "Total Buffer Pool Memory: " << this->_bufferPoolCapacity << " Bytes\n"
                << "Total Execution Pipeline Memory: " << this->_executionCapacity << " Bytes\n"
                << "Total Execution Pipeline Memory Used: " << executionUsed << " Bytes\n"
                << "Total Miscellaneous Memory: " << this->_miscCapacity << " Bytes\n"
                << "Total Miscellaneous Memory Used: " << miscUsed << " Bytes\n";
        }
        else if constexpr(LEVEL == ::Memory::MemoryLogLevel::KiloBytes){
            os  << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n"
                << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n"
                << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n"
                << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n"
                << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_KB) << " KiloBytes\n"
                << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_KB) << " KiloBytes\n";
        }
        else if (LEVEL == ::Memory::MemoryLogLevel::MegaBytes){
            os  << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n"
                << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n"
                << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n"
                << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n"
                << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_MB) << " MegaBytes\n"
                << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_MB) << " MegaBytes\n";
        }
        else{
            os  << "Total Database Memory: " << (this->_dbCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n"
                << "Total Buffer Pool Memory: " << (this->_bufferPoolCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n"
                << "Total Execution Pipeline Memory: " << (this->_executionCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n"
                << "Total Execution Pipeline Memory Used: " << (executionUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n"
                << "Total Miscellaneous Memory: " << (this->_miscCapacity / ::Memory::BYTES_TO_GB) << " GigaBytes\n"
                << "Total Miscellaneous Memory Used: " << (miscUsed / ::Memory::BYTES_TO_GB) << " GigaBytes\n";
        }
    }
}
