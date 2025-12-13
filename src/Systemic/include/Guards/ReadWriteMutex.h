#pragma once
#include <mutex>
#include <condition_variable>

namespace MultiThreading {
  class ReadWriteMutex {
      std::mutex mutex;
      std::condition_variable readersCV;   // wake readers
      std::condition_variable writersCV;   // wake writers
      int readers;                          // number of active readers
      bool writerActive;                   // is a writer active?
      int writersWaiting;                  // number of waiting writers
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

