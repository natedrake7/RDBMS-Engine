#include "BufferPool/BufferPoolMemoryManager.h"
#include <cstring>

#include "Managers/GlobalMemoryManager.h"
#include "Pages/Additional/Frame.h"

namespace CoreEngine{
    BufferPoolMemoryManager::BufferPoolMemoryManager(){
        this->_data = nullptr;
        this->_framesData = nullptr;
        this->_capacity = 0;
        this->_freeStack = nullptr;
        this->_freeTopId = 0;
    }

    BufferPoolMemoryManager::~BufferPoolMemoryManager(){
#ifdef _WIN32
        _aligned_free(this->_data);
        _aligned_free(this->_framesData);
#else
        std::free(this->_data);
        std::free(this->_framesData);
#endif
    }

    BufferPoolMemoryManager& BufferPoolMemoryManager::Get(){
        static BufferPoolMemoryManager _instance;
        return _instance;
    }

    void BufferPoolMemoryManager::Initialize(const UnsignedBigInt size){
        this->_capacity = static_cast<Int>(size / (Constants::PAGE_SIZE + sizeof(Pages::Frame)));

        this->AllocatePagePool();
        this->AllocateFramePool();
        this->AllocateFreeStack();
    }

    Int BufferPoolMemoryManager::Capacity() const{ return this->_capacity; }

    void BufferPoolMemoryManager::AllocatePagePool(){
#ifdef _WIN32
        this->_data =  static_cast<object_t*>(_aligned_malloc(this->_capacity * Constants::PAGE_SIZE, Constants::PAGE_SIZE));
#else
        this->_data = static_cast<object_t*>(std::aligned_alloc(Constants::PAGE_SIZE, this->_capacity * Constants::PAGE_SIZE));
#endif

        std::memset(this->_data, 0, this->_capacity * Constants::PAGE_SIZE);
    }

    void BufferPoolMemoryManager::AllocateFramePool(){
#ifdef _WIN32
        this->_framesData =  static_cast<Pages::Frame*>(_aligned_malloc(this->_capacity * sizeof(Pages::Frame),alignof(Pages::Frame)));
#else
        this->_framesData = static_cast<Pages::Frame*>(std::aligned_alloc(alignof(Pages::Frame), this->_capacity * sizeof(Pages::Frame)));
#endif
    }

    void BufferPoolMemoryManager::AllocateFreeStack(){
#ifdef _WIN32
        this->_freeStack =  static_cast<Pages::FrameId*>(_aligned_malloc(this->_capacity * sizeof(Pages::FrameId),alignof(Pages::FrameId)));
#else
        this->_freeStack = static_cast<Storage::FrameId*>(std::aligned_alloc(alignof(Storage::FrameId), this->_capacity * sizeof(Storage::FrameId)));
#endif
        for (Int i = 0; i < this->_capacity; ++i)
            this->_freeStack[i] = i;

        this->_freeTopId = this->_capacity;
    }

    object_t* BufferPoolMemoryManager::Data() const{
        return this->_data;
    }

    object_t* BufferPoolMemoryManager::Data(const UnsignedBigInt offset) const{
        return this->_data + offset;
    }

    object_t* BufferPoolMemoryManager::CopyToMemory(const char* buffer, const UnsignedBigInt offset, const page_offset_t bufferOffset) const{
        auto* destination = this->_data + offset;
        std::memcpy(destination, buffer + bufferOffset, Constants::PAGE_SIZE);
        return destination;
    }

    Pages::Frame* BufferPoolMemoryManager::AllocateFrame(const Int index) const{
        return &this->_framesData[index];
    }

    Pages::Frame* BufferPoolMemoryManager::GetFrame(const Int index) const{ return &this->_framesData[index]; }

    bool BufferPoolMemoryManager::PopStackNoLock(Pages::FrameId& frameId){
        if (this->_freeTopId == 0)
            return false;
        frameId = this->_freeStack[--this->_freeTopId];
        return true;
    }

    void BufferPoolMemoryManager::PushStackNoLock(const Pages::FrameId frameId){
        this->_freeStack[this->_freeTopId++] = frameId;
    }
}
