#include "../../../QueryPipeline/include/CompileContext.h"

namespace QueryPipeline{
    CompileContext::CompileContext(const Int size){
        this->allocator = Memory::Allocator(size);
        this->statements.SetAllocator(this->allocator);
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

    void* CompileContext::Allocate(const Int size) const{ return this->allocator.Allocate(size); }
}
