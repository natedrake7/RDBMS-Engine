#pragma once
#include <atomic>
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/Memory/Functions.h"

namespace Memory{
    struct OSMemoryInfo;
}

namespace CoreEngine{
    class GlobalMemoryManager{
        UnsignedBigInt _dbCapacity;
        UnsignedBigInt _bufferPoolCapacity;
        UnsignedBigInt _executionCapacity;
        UnsignedBigInt _miscCapacity;

        std::atomic<UnsignedBigInt> _executionUsed;
        std::atomic<UnsignedBigInt> _miscUsed;

        static constexpr Int DB_RATIO_NUMERATOR = 60;
        static constexpr Int DB_RATIO_DENOMINATOR = 100;

        static constexpr Int BUFFER_POOL_RATIO_NUMERATOR = 50;
        static constexpr Int BUFFER_POOL_RATIO_DENOMINATOR = 100;

        static constexpr Int EXECUTION_RATIO_NUMERATOR = 40;
        static constexpr Int EXECUTION_RATIO_DENOMINATOR = 100;

        static constexpr Int MISC_RATIO_NUMERATOR = 10;
        static constexpr Int MISC_RATIO_DENOMINATOR = 100;

        GlobalMemoryManager();
        public:
            GlobalMemoryManager(const GlobalMemoryManager&) = delete;
            GlobalMemoryManager& operator=(const GlobalMemoryManager&) = delete;
            GlobalMemoryManager(GlobalMemoryManager&&) = delete;
            GlobalMemoryManager& operator=(GlobalMemoryManager&&) = delete;

            void Initialize(const ::Memory::OSMemoryInfo& memoryInfo);
            static GlobalMemoryManager& Get();

            [[nodiscard]] bool TryReserveForExecution(UnsignedBigInt size);
            void ReleaseExecutionReservation(UnsignedBigInt size);

            [[nodiscard]] bool TryReserveForMisc(UnsignedBigInt size);
            void ReleaseMiscReservation(UnsignedBigInt size);

            UnsignedBigInt GetDbCapacity()const;
            UnsignedBigInt GetBufferPoolCapacity()const;
            UnsignedBigInt GetMiscCapacity()const;
            UnsignedBigInt GetMiscReservation()const;
            UnsignedBigInt GetExecutionCapacity()const;
            UnsignedBigInt GetExecutionReservation()const;

            void Log(std::ostream& os, ::Memory::MemoryLogLevel level)const;
    };
}
