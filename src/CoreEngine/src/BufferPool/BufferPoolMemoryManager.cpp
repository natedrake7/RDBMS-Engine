#include "BufferPool/BufferPoolMemoryManager.h"
#include <cstring>

#include "Managers/GlobalMemoryManager.h"
#include "Pages/Additional/Frame.h"

namespace CoreEngine{
    BufferPoolMemoryManager::BufferPoolMemoryManager(){
        this->_data = nullptr;
        this->_framesData = nullptr;
        this->_framesCount = 0;
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
        this->_framesCount = size / (Constants::PAGE_SIZE + sizeof(Pages::Frame));

        this->AllocatePagePool();
        this->AllocateFramePool();
    }

    Int BufferPoolMemoryManager::FramesCount() const{ return this->_framesCount; }

    void BufferPoolMemoryManager::AllocatePagePool(){
#ifdef _WIN32
        this->_data =  static_cast<object_t*>(_aligned_malloc(this->_framesCount * Constants::PAGE_SIZE, Constants::PAGE_SIZE));
#else
        this->_data = static_cast<object_t*>(std::aligned_alloc(Constants::PAGE_SIZE, this->_framesCount * Constants::PAGE_SIZE));
#endif

        std::memset(this->_data, 0, this->_framesCount * Constants::PAGE_SIZE);
    }

    void BufferPoolMemoryManager::AllocateFramePool(){
#ifdef _WIN32
        this->_framesData =  static_cast<Pages::Frame*>(_aligned_malloc(this->_framesCount * sizeof(Pages::Frame),alignof(Pages::Frame)));
#else
        this->_framesData = static_cast<Pages::Frame*>(std::aligned_alloc(alignof(Pages::Frame), this->_framesCount * sizeof(Pages::Frame)));
#endif
    }

    object_t* BufferPoolMemoryManager::Data() const{
        return this->_data;
    }

    object_t* BufferPoolMemoryManager::CopyToMemory(const char* buffer, const UnsignedBigInt offset, const page_offset_t bufferOffset) const{
        auto* destination = this->_data + offset;
        std::memcpy(destination, buffer + bufferOffset, Constants::PAGE_SIZE);
        return destination;
    }

    Pages::Frame* BufferPoolMemoryManager::GetFrame(const Int index) const{ return &this->_framesData[index]; }
}
