#include <Systemic/Guards/ReaderGuard.h>
#include <Systemic/Guards/Mutex.h>

namespace MultiThreading {
    ReaderGuard::ReaderGuard()
        : _mutex(nullptr){}

    ReaderGuard::ReaderGuard(Mutex* mtx)
        : _mutex(mtx){
        this->_mutex->SharedLock();
    }

    ReaderGuard::~ReaderGuard(){
        if (this->_mutex == nullptr)
            return;

        this->_mutex->SharedUnlock();
    }

    ReaderGuard & ReaderGuard::operator=(ReaderGuard &&other) noexcept{
        if (this == &other)
            return *this;

        if (this->_mutex != nullptr)
            this->_mutex->SharedUnlock();

        this->_mutex = other._mutex;
        other._mutex = nullptr;

        return *this;
    }

    ReaderGuard::ReaderGuard(ReaderGuard &&other) noexcept
        : _mutex(other._mutex){
        other._mutex = nullptr;
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
        this->_mutex = mtx;
    }

    void ReaderGuard::Release()const{
        this->_mutex->SharedUnlock();
    }

    void ReaderGuard::DisableMutex(){
        this->_mutex = nullptr;
    }
}
