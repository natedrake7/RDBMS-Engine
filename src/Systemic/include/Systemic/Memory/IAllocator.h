#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <utility>

#include "Alignment.h"

namespace Memory {

    struct AllocationStep{
        void* _chunkAddress;
        UnsignedInt _chunkOffset;

        explicit AllocationStep(void* chunkAddress, const UnsignedInt chunkOffset) :
            _chunkAddress(chunkAddress), _chunkOffset(chunkOffset) {}

        [[nodiscard]] static AllocationStep DefaultStep(){
            return AllocationStep(nullptr, 0);
        }
    };

    class IAllocator {
        public:
            virtual ~IAllocator() = default;

            // Non-virtual so the default alignment lives in one place and applies whatever the
            // static type of the allocator is; implementations override AllocateAligned.
            [[nodiscard]] void* AllocateRaw(const UnsignedInt size, const std::size_t alignment = DEFAULT_ALIGNMENT) const{
                return this->AllocateAligned(size, alignment);
            }

            template<typename T> requires(
                std::is_trivially_copyable_v<T>
                && std::is_default_constructible_v<T>
                && !std::is_pointer_v<T>
            )
            [[nodiscard]] T* AllocateRaw(const UnsignedInt size) const{
                return static_cast<T*>(this->AllocateAligned(size, alignof(T)));
            }

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            virtual void Release() const = 0;
            virtual void Reset() const = 0;

            [[nodiscard]] virtual AllocationStep RecordAllocationStart() const = 0;
            virtual void ReleaseFromAllocationStep(AllocationStep& step) const = 0;

            template<typename Function>
            void ScopedExecution(Function&& func) const{
                auto allocationStep = RecordAllocationStart();
                func();
                ReleaseFromAllocationStep(allocationStep);
            }

        protected:
            [[nodiscard]] virtual void* AllocateAligned(UnsignedInt size, std::size_t alignment) const = 0;
    };

    template <typename Entity, typename... Args>
    Entity* IAllocator::Allocate(Args&&... args) const{
        return new (this->AllocateRaw(sizeof(Entity), alignof(Entity))) Entity(std::forward<Args>(args)...);
    }
}
