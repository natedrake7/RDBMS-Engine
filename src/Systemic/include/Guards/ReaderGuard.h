#pragma once
namespace MultiThreading {
  class ReadWriteMutex;

  class ReaderGuard {
    ReadWriteMutex* mutex;

    public:
      ReaderGuard();
      explicit ReaderGuard(ReadWriteMutex* mtx);
      ~ReaderGuard();

      ReaderGuard& operator=(const ReaderGuard& other) = delete;
      ReaderGuard(const ReaderGuard& other) = delete;

      ReaderGuard& operator=(ReaderGuard&& other)noexcept;
      ReaderGuard(ReaderGuard&& other) noexcept;

      static ReaderGuard TryLock(ReadWriteMutex* mtx, bool& isSuccessful);

      void SetMutex(ReadWriteMutex* mtx);

      void Release()const;
      void DisableMutex();
  };
}
