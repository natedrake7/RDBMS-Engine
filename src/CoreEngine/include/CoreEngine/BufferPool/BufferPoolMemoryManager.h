#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;
        StorageLayout::frame_id_t* _freeStack;
        UnsignedInt _freeTopId;

        UnsignedInt _capacity;


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

            [[nodiscard]] UnsignedInt Capacity() const{
                return this->_capacity;
            }

            [[nodiscard]] inline object_t* Data() const{
                return this->_data;
            }

            [[nodiscard]] inline object_t* Data(const UnsignedBigInt offset) const{
                return this->_data + offset;
            }

            object_t* CopyToMemory(const char* buffer, UnsignedBigInt offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* GetFrame(const UnsignedInt index) const{
                return &this->_framesData[index];
            }

            [[nodiscard]] bool PopStackNoLock(StorageLayout::frame_id_t& frameId);
            void PushStackNoLock(StorageLayout::frame_id_t frameId);
    };

}
