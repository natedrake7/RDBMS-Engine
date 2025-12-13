#include "../../../include/Guards/ReadWriteMutex.h"

namespace MultiThreading {
 ReadWriteMutex::ReadWriteMutex(){
  this->writersWaiting = 0;
  this->readers = 0;
  this->writerActive = false;
 }

 ReadWriteMutex::~ReadWriteMutex() = default;

 void ReadWriteMutex::SharedLock(){
   std::unique_lock<std::mutex> lk(this->mutex);
   // Wait while a writer is active OR writers waiting and we prefer writers
   this->readersCV.wait(lk, [this](){ return !this->writerActive && this->writersWaiting == 0; });
   this->readers++;
}

void ReadWriteMutex::SharedUnlock(){
  std::unique_lock<std::mutex> lk(mutex);

  this->readers--;

  if (this->readers == 0)
   this->writersCV.notify_one();
 }

void ReadWriteMutex::UniqueLock(){
   std::unique_lock<std::mutex> lk(this->mutex);

   this->writersWaiting++;
   this->writersCV.wait(lk, [this](){ return !this->writerActive && this->readers == 0; });
   this->writersWaiting--;
   this->writerActive = true;
}
void ReadWriteMutex::UniqueUnlock(){
   std::unique_lock<std::mutex> lk(this->mutex);
   this->writerActive = false;
   // Prefer waking a writer first to avoid writer starvation
   if (this->writersWaiting > 0) {
     this->writersCV.notify_one();
     return;
   }

  this->readersCV.notify_all();
}

bool ReadWriteMutex::SharedTryLock(){
   std::unique_lock<std::mutex> lk(this->mutex);

   if (this->writerActive || this->writersWaiting > 0)
     return false;

   // Wait while a writer is active OR writers waiting and we prefer writers
   this->readersCV.wait(lk, [this](){ return !this->writerActive && this->writersWaiting == 0; });
   this->readers++;

   return true;
}

bool ReadWriteMutex::UniqueTryLock(){
   std::unique_lock<std::mutex> lk(this->mutex);

   if (this->writerActive || this->writersWaiting > 0 || this->readers > 0)
     return false;

   this->writersWaiting++;
   this->writersCV.wait(lk, [this](){ return !this->writerActive && this->readers == 0; });
   this->writersWaiting--;
   this->writerActive = true;

   return true;
}

void ReadWriteMutex::PromoteLock(){
   std::unique_lock<std::mutex> lk(this->mutex);

   this->readers--;
   this->writersWaiting++;

   this->writersCV.wait(lk, [this](){ return !this->writerActive && this->readers == 0; });
   this->writersWaiting--;
   this->writerActive = true;
}
}
