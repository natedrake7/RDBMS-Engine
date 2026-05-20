#pragma once
namespace MultiThreading {
  class Mutex;

  class ReaderGuard {
    Mutex* mutex;

    public:
      ReaderGuard();
      explicit ReaderGuard(Mutex* mtx);
      ~ReaderGuard();

      ReaderGuard& operator=(const ReaderGuard& other) = delete;
      ReaderGuard(const ReaderGuard& other) = delete;

      ReaderGuard& operator=(ReaderGuard&& other)noexcept;
      ReaderGuard(ReaderGuard&& other) noexcept;

      static ReaderGuard TryLock(Mutex* mtx, bool& isSuccessful);

      void SetMutex(Mutex* mtx);

      void Release()const;
      void DisableMutex();
  };
}
