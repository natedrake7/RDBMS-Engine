#pragma once

namespace MultiThreading {
  class ReadWriteMutex;

  class WriterGuard {
    ReadWriteMutex* mutex;

    public:
      explicit WriterGuard(ReadWriteMutex* mtx);
      ~WriterGuard();
  };

}

