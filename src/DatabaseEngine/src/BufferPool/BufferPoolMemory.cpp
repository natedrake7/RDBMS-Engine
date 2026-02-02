#include "BufferPool/BufferPoolMemory.h"
#include <cstring>

namespace DatabaseEngine{
    BufferPoolMemory::BufferPoolMemory(const Int numberOfPages){
#ifdef _WIN32
        this->_data =  static_cast<char*>(_aligned_malloc(numberOfPages * Constants::PAGE_SIZE, Constants::PAGE_SIZE));
#elif

#endif

        std::memset(this->_data, 0, numberOfPages * Constants::PAGE_SIZE);
    }

    BufferPoolMemory::~BufferPoolMemory(){
#ifdef _WIN32
        _aligned_free(this->_data);
#elif

#endif
    }

    char* BufferPoolMemory::Data() const{
        return this->_data;
    }
}
