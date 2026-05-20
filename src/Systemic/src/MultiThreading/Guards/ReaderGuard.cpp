#include "../../../include/Guards/ReaderGuard.h"
#include "../../../include/Guards/Mutex.h"

namespace MultiThreading {
    ReaderGuard::ReaderGuard(){
        this->mutex = nullptr;
    }

    ReaderGuard::ReaderGuard(Mutex* mtx){
        this->mutex = mtx;
        this->mutex->SharedLock();
    }

    ReaderGuard::~ReaderGuard(){
        if (this->mutex == nullptr)
            return;

        this->mutex->SharedUnlock();
    }

    ReaderGuard & ReaderGuard::operator=(ReaderGuard &&other) noexcept{
        if (this == &other)
            return *this;

        if (this->mutex != nullptr)
            this->mutex->SharedUnlock();

        this->mutex = other.mutex;
        other.mutex = nullptr;

        return *this;
    }

    ReaderGuard::ReaderGuard(ReaderGuard &&other) noexcept{
        if (this == &other)
            return;

        if (this->mutex != nullptr)
            this->mutex->SharedUnlock();

        this->mutex = other.mutex;
        other.mutex = nullptr;
    }

    ReaderGuard ReaderGuard::TryLock(Mutex *mtx, bool& isSuccessful){
        auto guard = ReaderGuard();

        guard.SetMutex(mtx);

        isSuccessful = mtx->SharedTryLock();
        if (!isSuccessful)
            guard.DisableMutex();

        return guard;
    }

    void ReaderGuard::SetMutex(Mutex *mtx){
        this->mutex = mtx;
    }

    void ReaderGuard::Release()const{
        this->mutex->SharedUnlock();
    }

    void ReaderGuard::DisableMutex(){
        this->mutex = nullptr;
    }
}
