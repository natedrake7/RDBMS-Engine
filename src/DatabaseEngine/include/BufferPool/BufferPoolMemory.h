#pragma once
#include "../DatabaseConstants.h"

namespace DatabaseEngine{
    class BufferPoolMemory{
        object_t* _data;

        public:
            explicit BufferPoolMemory();
            ~BufferPoolMemory();

            void Allocate(Int numberOfPages);

            [[nodiscard]] object_t* Data() const;

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

    };

}
