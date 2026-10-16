#include <CoreEngine/BufferPool/BufferPoolMemoryManager.h>
#include <cstring>
#include <memory>

#include <CoreEngine/Managers/GlobalMemoryManager.h>
#include <CoreEngine/Pages/Additional/Frame.h>

#include <CoreEngine/BufferPool/StorageLayout.h>

namespace CoreEngine{
    void BufferPoolMemoryManager::AllocatePagePool(){
#ifdef _WIN32
        this->_data =  static_cast<object_t*>(_aligned_malloc(this->_capacity * StorageLayout::PAGE_SIZE, StorageLayout::PAGE_SIZE));
#else
        this->_data = static_cast<object_t*>(std::aligned_alloc(StorageLayout::PAGE_SIZE, this->_capacity * StorageLayout::PAGE_SIZE));
#endif

        std::memset(this->_data, 0, this->_capacity * StorageLayout::PAGE_SIZE);
    }

    void BufferPoolMemoryManager::AllocateFramePool(){
#ifdef _WIN32
        this->_framesData =  static_cast<Pages::Frame*>(_aligned_malloc(this->_capacity * sizeof(Pages::Frame),alignof(Pages::Frame)));
#else
        this->_framesData = static_cast<Pages::Frame*>(std::aligned_alloc(alignof(Pages::Frame), this->_capacity * sizeof(Pages::Frame)));
#endif
        // Raw memory is not a Frame: the latch (std::mutex + condition_variables) must be constructed.
        // libstdc++/winpthreads mutexes are invalid when zero-filled (PTHREAD_MUTEX_INITIALIZER is -1).
        std::uninitialized_default_construct_n(this->_framesData, this->_capacity);
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

    BufferPoolMemoryManager::~BufferPoolMemoryManager(){
        // Frames are intentionally not destroyed: this singleton only dies at process exit, and
        // destroying ~500k winpthreads mutexes/condvars is one CloseHandle round-trip each
        // (~0.25 s normally, ~50 s with a debugger attached). The OS reclaims the handles.
#ifdef _WIN32
        _aligned_free(this->_data);
        _aligned_free(this->_framesData);
#else
        std::free(this->_data);
        std::free(this->_framesData);
#endif
    }

    void BufferPoolMemoryManager::Initialize(const UnsignedBigInt size){
        this->_capacity = static_cast<Int>(size / (StorageLayout::PAGE_SIZE + sizeof(Pages::Frame)));

        this->AllocatePagePool();
        this->AllocateFramePool();
        this->AllocateFreeStack();
    }

    object_t* BufferPoolMemoryManager::CopyToMemory(const char* buffer, const UnsignedBigInt offset, const page_offset_t bufferOffset) const{
        auto* destination = this->_data + offset;
        std::memcpy(destination, buffer + bufferOffset, StorageLayout::PAGE_SIZE);
        return destination;
    }

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
