#pragma once
#include <atomic>

namespace CoreEngine {
  class GarbageCollector {
    public:
      static void Collect(const std::atomic<bool>& isServerRunning);
  };
}