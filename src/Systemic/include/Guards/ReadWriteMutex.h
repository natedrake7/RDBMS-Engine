#pragma once
#include <mutex>
#include <condition_variable>

namespace MultiThreading {
  class ReadWriteMutex {
      // std::mutex mutex;
      //
      // std::condition_variable readersCV;   // wake readers
      // std::condition_variable writersCV;   // wake writers
      //
      // int readers;                          // number of active readers
      // int writersWaiting;                  // number of waiting writers
      //
      // bool writerActive;                   // is a writer active?
      std::atomic<int> state{0};
      std::atomic<int> writersWaiting{0};
    public:
      ReadWriteMutex();
      ~ReadWriteMutex();

    void SharedLock();
    void SharedUnlock();
    void UniqueLock();
    void UniqueUnlock();

    bool SharedTryLock();
    bool UniqueTryLock();
    void PromoteLock();
  };

}

