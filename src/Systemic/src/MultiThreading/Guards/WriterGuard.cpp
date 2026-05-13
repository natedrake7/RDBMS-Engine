#include "../../../include/Guards/WriterGuard.h"
#include "../../../include/Guards/ReaderGuard.h"
#include "../../../include/Guards/ReadWriteMutex.h"

namespace MultiThreading {
    WriterGuard::WriterGuard(ReadWriteMutex *mtx){
        this->mutex = mtx;
        this->mutex->UniqueLock();
    }

    WriterGuard::WriterGuard() {
        this->mutex = nullptr;
    }

    WriterGuard::~WriterGuard(){
        if (this->mutex != nullptr)
            this->mutex->UniqueUnlock();
    }

    WriterGuard::WriterGuard(WriterGuard &&other) noexcept {
        if (this == &other)
            return;

        if (this->mutex != nullptr)
            this->mutex->UniqueUnlock();

        this->mutex = other.mutex;
        other.mutex = nullptr;
    }

    WriterGuard & WriterGuard::operator=(WriterGuard &&other) noexcept {
        if (this == &other)
            return *this;

        if (this->mutex != nullptr)
            this->mutex->UniqueUnlock();

        this->mutex = other.mutex;
        other.mutex = nullptr;

        return *this;
    }

    void WriterGuard::PromoteLock()const{
        this->mutex->PromoteLock();
    }

  void WriterGuard::SetMutex(ReadWriteMutex *mtx) {
    this->mutex = mtx;
  }

  WriterGuard WriterGuard::Promote(ReadWriteMutex *mtx, ReaderGuard& readGuard) {
    auto guard = WriterGuard();

    readGuard.DisableMutex();

    guard.SetMutex(mtx);
    guard.PromoteLock();

    return guard;
  }

  WriterGuard WriterGuard::TryLock(ReadWriteMutex *mtx, bool& isSuccessful) {
    auto guard = WriterGuard();

    guard.SetMutex(mtx);

    isSuccessful = mtx->UniqueTryLock();
    if (!isSuccessful)
      guard.DisableMutex();

    return guard;
  }

  void WriterGuard::DisableMutex() {
    this->mutex = nullptr;
  }

  void WriterGuard::Release()const {
    this->mutex->UniqueUnlock();
  }

}
