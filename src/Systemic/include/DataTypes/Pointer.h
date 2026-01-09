#pragma once
#include "../Guards/ReadWriteMutex.h"
#include "../Guards/WriterGuard.h"
#include "../Guards/ReaderGuard.h"

template <typename T>
class Pointer{
    T* _ptr;
    const Pointer* _ownerPtr;

    mutable int references;
    mutable MultiThreading::ReadWriteMutex _referencesMutex;

    public:
    Pointer()
        : _ptr(nullptr), _ownerPtr(nullptr), references(0) {}

    explicit Pointer(T* ptr)
        : _ptr(ptr), _ownerPtr(nullptr), references(1) {}

    ~Pointer(){
        if (this->IsOwner())
            delete this->_ptr;

        if (this->_ownerPtr != nullptr){
            this->_ownerPtr->DecreaseReference();
        }
    }

    Pointer(const Pointer& other){
        this->_ptr = other._ptr;
        this->_ownerPtr = &other;

        this->references = other.IncreaseReference();
    }

    Pointer& operator=(const Pointer& other){
        if (this == &other)
            return *this;

        if (this->_ownerPtr != nullptr)
            this->_ownerPtr->DecreaseReference();

        this->_ptr = other._ptr;
        this->_ownerPtr = &other;
        this->references = other.IncreaseReference();

        return *this;
    }

    Pointer(Pointer&& other) noexcept{
        this->_ptr = other._ptr;
        this->_ownerPtr = other._ownerPtr;
        this->references = other.references;

        other._ptr = nullptr;
        other.references = 0;
        other._ownerPtr = nullptr;
    }

    Pointer& operator=(Pointer&& other) noexcept{
        if (this == &other)
            return *this;

        if (this->_ownerPtr != nullptr)
            this->_ownerPtr->DecreaseReference();

        this->_ptr = other._ptr;
        this->_ownerPtr = other._ownerPtr;
        this->references = other.references;

        other._ptr = nullptr;
        other.references = 0;
        other._ownerPtr = nullptr;

        return *this;
    }

    int IncreaseReference() const {
        MultiThreading::WriterGuard guard(&this->_referencesMutex);
        this->references += 1;
        return this->references;
    }

    void DecreaseReference() const{
        MultiThreading::WriterGuard guard(&this->_referencesMutex);
        this->references -= 1;
    }

    int GetReference() const{
        MultiThreading::ReaderGuard guard(&this->_referencesMutex);
        return this->references;
    }

    bool IsOwner() const{
        return this->_ownerPtr == nullptr;
    }

    T* operator->() { return this->_ptr; }
    T& operator*() { return *this->_ptr; }

    T* operator->() const { return this->_ptr; }
    T& operator*() const { return *this->_ptr; }

    T* Get() { return this->_ptr; }
    T* Get() const { return this->_ptr; }
};