#include "ReaderGuard.h"
#include "../../ReadWriteMutex/ReadWriteMutex.h"

namespace MultiThreading {

ReaderGuard::ReaderGuard(ReadWriteMutex* mtx){
  this->mutex = mtx;
  this->mutex->SharedLock();
}

ReaderGuard::~ReaderGuard(){
  this->mutex->SharedUnlock();
}

}
