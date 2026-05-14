#include "../../../include/Guards/ReadWriteMutex.h"

namespace MultiThreading {
    ReadWriteMutex::ReadWriteMutex(){
        this->writersWaiting.store(0);
        this->state.store(0);
        // this->readers = 0;
        // this->writerActive = false;
    }

    ReadWriteMutex::~ReadWriteMutex() = default;

    void ReadWriteMutex::SharedLock(){
        while (true) {
            // Spin if writer active or writers waiting
            while (this->state.load(std::memory_order_acquire) < 0
                || this->writersWaiting.load(std::memory_order_acquire) > 0
            ) std::this_thread::yield();

            auto expected = this->state.load(std::memory_order_relaxed);
            if (expected >= 0
                && this->state.compare_exchange_weak(expected, expected + 1,
                    std::memory_order_acquire, std::memory_order_relaxed))
                return;
        }
        // std::unique_lock lk(this->mutex);
        // // Wait while a writer is active OR writers waiting and we prefer writers
        // this->readersCV.wait(lk, [this]()
        // {
        // return !this->writerActive && this->writersWaiting == 0;
        // });
        // this->readers++;
    }

    void ReadWriteMutex::SharedUnlock(){
        this->state.fetch_sub(1, std::memory_order_release);
        // std::unique_lock lk(mutex);
        //
        // this->readers--;
        //
        // if (this->readers == 0)
        // this->writersCV.notify_one();
    }

    void ReadWriteMutex::UniqueLock(){
        this->writersWaiting.fetch_add(1, std::memory_order_relaxed);
        int expected = 0;
        while (!this->state.compare_exchange_weak(expected, -1,
            std::memory_order_acquire, std::memory_order_relaxed)) {
            expected = 0;
            std::this_thread::yield();
            }
        this->writersWaiting.fetch_sub(1, std::memory_order_relaxed);
        // std::unique_lock lk(this->mutex);
        //
        // this->writersWaiting++;
        // this->writersCV.wait(lk, [this]()
        // {
        // return !this->writerActive && this->readers == 0;
        // });
        // this->writersWaiting--;
        // this->writerActive = true;
    }

    void ReadWriteMutex::UniqueUnlock(){
        this->state.store(0, std::memory_order_release);
        // std::unique_lock lk(this->mutex);
        // this->writerActive = false;
        // // Prefer waking a writer first to avoid writer starvation
        // if (this->writersWaiting > 0) {
        // this->writersCV.notify_one();
        // return;
        // }
        //
        // this->readersCV.notify_all();
    }

    bool ReadWriteMutex::SharedTryLock(){
        if (this->writersWaiting.load(std::memory_order_acquire) > 0) return false;
        auto expected = this->state.load(std::memory_order_relaxed);
        if (expected < 0) return false;
        return this->state.compare_exchange_strong(expected, expected + 1,
            std::memory_order_acquire, std::memory_order_relaxed);
   // std::unique_lock lk(this->mutex);
   //
   // if (this->writerActive || this->writersWaiting > 0)
   //   return false;
   //
   // // Wait while a writer is active OR writers waiting and we prefer writers
   // this->readersCV.wait(lk, [this]()
   // {
   //     return !this->writerActive && this->writersWaiting == 0;
   // });
   // this->readers++;
   //
   // return true;
    }

    bool ReadWriteMutex::UniqueTryLock(){
        auto expected = 0;
        return this->state.compare_exchange_strong(expected, -1,
        std::memory_order_acquire, std::memory_order_relaxed);
    }

    void ReadWriteMutex::PromoteLock(){
        // Promote: already hold shared lock (state >= 1), want exclusive
        // Atomically go from 1 reader (us) to writer (-1)
        auto expected = 1;
        while (!this->state.compare_exchange_weak(expected, -1,
            std::memory_order_acquire, std::memory_order_relaxed)) {
            // Other readers still active, wait for them to drain
            expected = 1;
            std::this_thread::yield();
        }
    }
}
