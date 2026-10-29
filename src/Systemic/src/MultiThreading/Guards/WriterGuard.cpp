#include <Systemic/Guards/WriterGuard.h>
#include <Systemic/Guards/Mutex.h>

namespace MultiThreading {
    WriterGuard::WriterGuard(Mutex *mtx)
        : _mutex(mtx){
        this->_mutex->UniqueLock();
    }

    WriterGuard::WriterGuard()
        : _mutex(nullptr){}

    WriterGuard::~WriterGuard(){
        if (this->_mutex != nullptr)
            this->_mutex->UniqueUnlock();
    }

    WriterGuard::WriterGuard(WriterGuard &&other) noexcept
        : _mutex(other._mutex){
        other._mutex = nullptr;
    }

    WriterGuard& WriterGuard::operator=(WriterGuard &&other) noexcept {
        if (this == &other)
            return *this;

        if (this->_mutex != nullptr)
            this->_mutex->UniqueUnlock();

        this->_mutex = other._mutex;
        other._mutex = nullptr;

        return *this;
    }

    void WriterGuard::PromoteLock()const{
        this->_mutex->PromoteLock();
    }

    void WriterGuard::SetMutex(Mutex *mtx) {
        this->_mutex = mtx;
    }

    WriterGuard WriterGuard::TryLock(Mutex *mtx, bool& isSuccessful) {
        WriterGuard guard;
        guard.SetMutex(mtx);

        isSuccessful = mtx->UniqueTryLock();
        if (!isSuccessful)
            guard.DisableMutex();

        return guard;
    }

    void WriterGuard::DisableMutex() {
        this->_mutex = nullptr;
    }

    void WriterGuard::Release()const {
        this->_mutex->UniqueUnlock();
    }

}
