#include "../../../QueryPipeline/include/CompileContext.h"
#include "Managers/GlobalMemoryManager.h"

namespace QueryPipeline{
    CompileContext::CompileContext(const Int size){
        this->allocator = Memory::Allocator(size);

        if (!DatabaseEngine::GlobalMemoryManager::Get().TryReserveForExecution(size))
            throw std::bad_alloc();

        this->statements.SetAllocator(this->allocator);
    }

    CompileContext::~CompileContext(){
        // DatabaseEngine::GlobalMemoryManager::Get().ReleaseExecutionReservation(this->allocator.GetCapacity());
    }

    CompileContext::CompileContext(CompileContext&& other) noexcept{
        this->allocator = std::move(other.allocator);
        this->statements = std::move(other.statements);
    }

    CompileContext& CompileContext::operator=(CompileContext&& other) noexcept{
        if (this == &other)
            return *this;

        this->allocator = std::move(other.allocator);
        this->statements = std::move(other.statements);
        return *this;
    }

    void CompileContext::SetStatements(DataStructures::PolymorphicArray<Statements::Statement*>& otherStatements){
        this->statements = std::move(otherStatements);
    }

    void CompileContext::Reserve(const Int size){
        this->statements.Reserve(size);
    }

    void CompileContext::Push(Statements::Statement* statement){
        this->statements.Push(statement);
    }

    DataStructures::PolymorphicArray<Statements::Statement*>* CompileContext::GetStatements(){
        return &this->statements;
    }

    const Memory::Allocator& CompileContext::GetAllocator() const{
        return this->allocator;
    }

    void* CompileContext::Allocate(const Int size) const{
        // if (!DatabaseEngine::GlobalMemoryManager::Get().TryReserveForExecution(newCapacity - prevCapacity))
        //     throw std::bad_alloc();

        return this->allocator.Allocate(size);
    }
}
