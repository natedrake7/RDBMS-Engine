#pragma once
#include <atomic>

namespace DatabaseEngine {
  class GarbageCollector {
    public:
      static void Collect(const std::atomic<bool>& isServerRunning);
  };
}