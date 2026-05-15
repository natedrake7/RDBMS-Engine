#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace MultiThreading {
    class ReadWriteMutex {
        // state:
        //  0  -> unlocked
        // >0  -> reader count
        // -1  -> writer active
        std::mutex waitMutex;
        std::condition_variable readersCV;
        std::condition_variable writersCV;

        std::atomic<int> state;

        std::atomic<int> waitingReaders;
        std::atomic<int> waitingWriters;

        std::atomic<bool> upgradePending;

        static constexpr auto SPIN_COUNT = 64;

    private:
        static void cpu_pause() {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#else
            std::this_thread::yield();
#endif
        }

        bool TrySharedFast();
        bool TryUniqueFast();
        bool TryPromoteLock();

    public:
        ReadWriteMutex();
        ~ReadWriteMutex() = default;

        void SharedLock();
        void SharedUnlock();

        void UniqueLock();
        void UniqueUnlock();

        bool SharedTryLock();
        bool UniqueTryLock();

        void PromoteLock();
    };

}