#pragma once

namespace MultiThreading {
  class ReaderGuard;
  class ReadWriteMutex;

  class WriterGuard {
    ReadWriteMutex* mutex;

    void PromoteLock()const;

    public:
      explicit WriterGuard(ReadWriteMutex* mtx);
      explicit WriterGuard();
      ~WriterGuard();

      WriterGuard(WriterGuard&& other)noexcept;
      WriterGuard& operator=(WriterGuard&& other)noexcept;

      void SetMutex(ReadWriteMutex* mtx);
      static WriterGuard Promote(ReadWriteMutex* mtx, ReaderGuard& readGuard);
      static WriterGuard TryLock(ReadWriteMutex* mtx, bool& isSuccessful);

      void DisableMutex();

      void Release()const;
  };

}

