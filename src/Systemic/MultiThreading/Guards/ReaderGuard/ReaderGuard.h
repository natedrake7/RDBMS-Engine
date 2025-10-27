#pragma once
namespace MultiThreading {
  class ReadWriteMutex;

  class ReaderGuard {
    ReadWriteMutex* mutex;

    public:
      explicit ReaderGuard(ReadWriteMutex* mtx);
      ~ReaderGuard();
  };
}
