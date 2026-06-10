#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../Pages/Additional/Frame.h"

namespace CoreEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;
        Pages::FrameId* _freeStack;
        Int _freeTopId;

        Int _capacity;

        explicit BufferPoolMemoryManager();
        ~BufferPoolMemoryManager();

        void AllocatePagePool();
        void AllocateFramePool();
        void AllocateFreeStack();

        public:
            static BufferPoolMemoryManager& Get();

            void Initialize(UnsignedBigInt size);

            [[nodiscard]] Int Capacity() const;
            [[nodiscard]] object_t* Data() const;
            [[nodiscard]] object_t* Data(UnsignedBigInt offset) const;

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* AllocateFrame(Int index) const;

            [[nodiscard]] Pages::Frame* GetFrame(Int index) const;

            [[nodiscard]] bool PopStackNoLock(Pages::FrameId& frameId);
            void PushStackNoLock(Pages::FrameId frameId);
    };

}
