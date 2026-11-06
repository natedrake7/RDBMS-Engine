#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <CoreEngine/Memory/Allocator.h>

namespace QueryPipeline{
    namespace Statements{
        struct Statement;
    }

    class CompileContext{
        CoreEngine::Memory::Allocator allocator;
        DataStructures::PolymorphicArray<Statements::Statement*> statements;

        static constexpr Int32_t DEFAULT_COMPILATION_ALLOCATION_SIZE = 1024 * 1024 * 2; //2MB

        public:
            explicit CompileContext(Int32_t size = DEFAULT_COMPILATION_ALLOCATION_SIZE);
            ~CompileContext();
            CompileContext(const CompileContext&) = delete;
            CompileContext& operator=(const CompileContext&) = delete;
            CompileContext(CompileContext&& other) noexcept;
            CompileContext& operator=(CompileContext&& other) noexcept;

            void SetStatements(DataStructures::PolymorphicArray<Statements::Statement*>& otherStatements);
            void Reserve(Int32_t size);
            void Push(Statements::Statement* statement);
            DataStructures::PolymorphicArray<Statements::Statement*>* GetStatements();
            const Memory::IAllocator* GetAllocator()const;

            void* Allocate(Int32_t size) const;
            template<typename T, typename... Args>
            T* Allocate(Args&&... args) const{
                return this->allocator.Allocate<T>(std::forward<Args>(args)...);
            }

    };
}
