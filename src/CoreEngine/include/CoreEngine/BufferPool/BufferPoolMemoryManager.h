#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine{
    class BufferPoolMemoryManager{
        object_t* _data;
        Pages::Frame* _framesData;
        StorageLayout::frame_id_t* _freeStack;
        UInt32_t _freeTopId;

        UInt32_t _capacity;


        void AllocatePagePool();
        void AllocateFramePool();
        void AllocateFreeStack();

        public:
            BufferPoolMemoryManager() = default;
            ~BufferPoolMemoryManager();

            BufferPoolMemoryManager(const BufferPoolMemoryManager& other) = delete;
            BufferPoolMemoryManager& operator=(const BufferPoolMemoryManager& other) = delete;
            BufferPoolMemoryManager(BufferPoolMemoryManager&& other) = delete;
            BufferPoolMemoryManager& operator=(BufferPoolMemoryManager&& other) = delete;

            void Initialize(UInt64_t size);

            [[nodiscard]] UInt32_t Capacity() const{
                return this->_capacity;
            }

            [[nodiscard]] inline object_t* Data() const{
                return this->_data;
            }

            [[nodiscard]] inline object_t* Data(const UInt64_t offset) const{
                return this->_data + offset;
            }

            object_t* CopyToMemory(const char* buffer, UInt64_t offset, page_offset_t bufferOffset) const;

            [[nodiscard]] Pages::Frame* GetFrame(const UInt32_t index) const{
                return &this->_framesData[index];
            }

            [[nodiscard]] object_t* FrameData(const UInt32_t index){
                return this->_data + index * StorageLayout::PAGE_SIZE;
            }

            [[nodiscard]] const object_t* FrameData(const UInt32_t index) const{
                return this->_data + index * StorageLayout::PAGE_SIZE;
            }

            [[nodiscard]] bool PopStackNoLock(StorageLayout::frame_id_t& frameId);
            void PushStackNoLock(StorageLayout::frame_id_t frameId);
    };

}
