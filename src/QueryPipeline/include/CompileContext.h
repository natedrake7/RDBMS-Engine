#pragma once
#include "Statements.h"
#include "../../Systemic/include/Memory/Allocator.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace QueryPipeline{
    class CompileContext{
        Memory::Allocator allocator;
        DataStructures::PolymorphicArray<Statements::Statement*> statements;

        static constexpr Int DEFAULT_COMPILATION_ALLOCATION_SIZE = 1024 * 1024 * 2; //2MB

        public:
            explicit CompileContext(Int size = DEFAULT_COMPILATION_ALLOCATION_SIZE);
            ~CompileContext();
            CompileContext(const CompileContext&) = delete;
            CompileContext& operator=(const CompileContext&) = delete;
            CompileContext(CompileContext&& other) noexcept;
            CompileContext& operator=(CompileContext&& other) noexcept;

            void SetStatements(DataStructures::PolymorphicArray<Statements::Statement*>& otherStatements);
            void Reserve(Int size);
            void Push(Statements::Statement* statement);
            DataStructures::PolymorphicArray<Statements::Statement*>* GetStatements();
            const Memory::Allocator& GetAllocator()const;

            void* Allocate(Int size) const;
            template<typename T>
            T* Allocate() const{
                return new (this->Allocate(sizeof(T))) T;
            }
            template<typename T, typename... Args>
            T* Allocate(Args&&... args) const{
                return new (this->Allocate(sizeof(T))) T(std::forward<Args>(args)...);
            }

    };
}