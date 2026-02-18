#pragma once
#include "../DatabaseConstants.h"

namespace Pages
{
    struct Frame;
}

namespace DatabaseEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;

        Int _framesCount;

        explicit BufferPoolMemoryManager();
        ~BufferPoolMemoryManager();

        void AllocatePagePool();
        void AllocateFramePool();

        public:
            static BufferPoolMemoryManager& Get();

            void Initialize(UnsignedBigInt size);

            [[nodiscard]] Int FramesCount() const;
            [[nodiscard]] object_t* Data() const;

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* GetFrame(Int index) const;
    };

}
