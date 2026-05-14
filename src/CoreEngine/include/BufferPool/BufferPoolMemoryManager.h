#pragma once
#include <atomic>

#include "../DatabaseConstants.h"

namespace Pages
{
    struct Frame;
}

namespace CoreEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;

        Int _capacity;
        std::atomic<Int> _size;

        explicit BufferPoolMemoryManager();
        ~BufferPoolMemoryManager();

        void AllocatePagePool();
        void AllocateFramePool();

        public:
            static BufferPoolMemoryManager& Get();

            void Initialize(UnsignedBigInt size);

            [[nodiscard]] Int Capacity() const;
            [[nodiscard]] object_t* Data() const;

            [[nodiscard]] bool IsFull() const;

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* AllocateFrame(Int index);

            void EvictFrame();
            [[nodiscard]] Pages::Frame* GetFrame(Int index) const;
    };

}
