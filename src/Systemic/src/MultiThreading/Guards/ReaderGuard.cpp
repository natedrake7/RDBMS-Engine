#include "../../../include/Guards/ReaderGuard.h"
#include "../../../include/Guards/ReadWriteMutex.h"

namespace MultiThreading {

ReaderGuard::ReaderGuard(ReadWriteMutex* mtx){
  this->mutex = mtx;
  this->mutex->SharedLock();
}

ReaderGuard::~ReaderGuard(){
  if (this->mutex == nullptr)
    return;

  this->mutex->SharedUnlock();
}

void ReaderGuard::Release()const{
  this->mutex->SharedUnlock();
}

void ReaderGuard::DisableMutex(){
  this->mutex = nullptr;
}

}
