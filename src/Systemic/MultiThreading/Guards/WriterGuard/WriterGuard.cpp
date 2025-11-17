#include "WriterGuard.h"

#include "../../ReadWriteMutex/ReadWriteMutex.h"

namespace MultiThreading {
  WriterGuard::WriterGuard(ReadWriteMutex *mtx){
    this->mutex = mtx;
    this->mutex->UniqueLock();
  }

  WriterGuard::~WriterGuard(){
    this->mutex->UniqueUnlock();
  }

  void WriterGuard::Release()const {
    this->mutex->UniqueUnlock();
  }

}
