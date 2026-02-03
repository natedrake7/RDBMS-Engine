#include "BufferPool/BufferPoolMemory.h"
#include <cstring>

namespace DatabaseEngine{
    BufferPoolMemory::BufferPoolMemory(){
        this->_data = nullptr;
    }

    BufferPoolMemory::~BufferPoolMemory(){
#ifdef _WIN32
        _aligned_free(this->_data);
#elif

#endif
    }

    void BufferPoolMemory::Allocate(const Int numberOfPages){
#ifdef _WIN32
        this->_data =  static_cast<object_t*>(_aligned_malloc(numberOfPages * Constants::PAGE_SIZE, Constants::PAGE_SIZE));
#elif

#endif

        std::memset(this->_data, 0, numberOfPages * Constants::PAGE_SIZE);
    }

    object_t* BufferPoolMemory::Data() const{
        return this->_data;
    }

    object_t* BufferPoolMemory::CopyToMemory(const char* buffer, const page_offset_t offset, const page_offset_t bufferOffset) const{
        auto* destination = this->_data + offset;
        std::memcpy(destination, buffer + bufferOffset, Constants::PAGE_SIZE);
        return destination;
    }
}
