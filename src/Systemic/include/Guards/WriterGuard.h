#pragma once

namespace MultiThreading {
  class ReaderGuard;
  class Mutex;

  class WriterGuard {
    Mutex* mutex;

    void PromoteLock()const;

    public:
      explicit WriterGuard(Mutex* mtx);
      explicit WriterGuard();
      ~WriterGuard();

      WriterGuard(const WriterGuard& other) = delete;
      WriterGuard& operator=(const WriterGuard& other) = delete;

      WriterGuard(WriterGuard&& other)noexcept;
      WriterGuard& operator=(WriterGuard&& other)noexcept;

      void SetMutex(Mutex* mtx);
      static WriterGuard TryLock(Mutex* mtx, bool& isSuccessful);

      void DisableMutex();

      void Release()const;
  };

}

