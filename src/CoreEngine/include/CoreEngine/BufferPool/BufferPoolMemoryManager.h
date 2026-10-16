#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;
        Pages::FrameId* _freeStack;
        Int _freeTopId;

        Int _capacity;


        void AllocatePagePool();
        void AllocateFramePool();
        void AllocateFreeStack();

        public:
            explicit BufferPoolMemoryManager() = default;
            ~BufferPoolMemoryManager();

            BufferPoolMemoryManager(const BufferPoolMemoryManager& other) = delete;
            BufferPoolMemoryManager& operator=(const BufferPoolMemoryManager& other) = delete;
            BufferPoolMemoryManager(BufferPoolMemoryManager&& other) = delete;
            BufferPoolMemoryManager& operator=(BufferPoolMemoryManager&& other) = delete;

            void Initialize(UnsignedBigInt size);

            [[nodiscard]] Int Capacity() const{
                return this->_capacity;
            }

            [[nodiscard]] inline object_t* Data() const{
                return this->_data;
            }

            [[nodiscard]] inline object_t* Data(const UnsignedBigInt offset) const{
                return this->_data + offset;
            }

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* GetFrame(const Int index) const{
                return &this->_framesData[index];
            }

            [[nodiscard]] bool PopStackNoLock(Pages::FrameId& frameId);
            void PushStackNoLock(Pages::FrameId frameId);
    };

}
