#pragma once
#include "../DatabaseConstants.h"

namespace DatabaseEngine{
    class BufferPoolMemory{
        char* _data;

        public:
            explicit BufferPoolMemory(Int numberOfPages);
            ~BufferPoolMemory();

            [[nodiscard]] char* Data() const;

    };

}
