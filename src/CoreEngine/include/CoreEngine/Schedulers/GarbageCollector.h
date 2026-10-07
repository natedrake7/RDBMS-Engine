#pragma once
#include <atomic>
#include <condition_variable>

namespace CoreEngine {
  class GarbageCollector {
      static inline std::condition_variable _cv;
      static inline std::mutex _mutex;
    public:
      static void Collect(const std::atomic<bool>& isServerRunning);
      static void Stop();
  };
}
