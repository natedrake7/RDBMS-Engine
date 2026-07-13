#include "../../../include/Guards/WriterGuard.h"
#include "../../../include/Guards/Mutex.h"

namespace MultiThreading {
    WriterGuard::WriterGuard(Mutex *mtx){
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

    WriterGuard& WriterGuard::operator=(WriterGuard &&other) noexcept {
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

    void WriterGuard::SetMutex(Mutex *mtx) {
        this->mutex = mtx;
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
        this->mutex = nullptr;
    }

    void WriterGuard::Release()const {
        this->mutex->UniqueUnlock();
    }

}
