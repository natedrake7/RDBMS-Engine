#include <CoreEngine/Managers/IdentityManager.h>

#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <Server/Server.h>
#include <Systemic/Guards/WriterGuard.h>

namespace CoreEngine::StorageTypes {
    void IdentityManager::ReserveBlock(const ::Memory::IAllocator* allocator, const BigInt value){
        MultiThreading::WriterGuard guard(&this->mutex);

        auto ceiling = this->_reservedUpTo.load(std::memory_order_relaxed);
        if (value < ceiling)
            return;

        do{
          ceiling += this->_schema->_cacheBlock;
        }while (value >= ceiling);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->_tableId,
            this->_columnId,
            ceiling
        );

        this->_reservedUpTo.store(ceiling, std::memory_order_relaxed);
    }

    IdentityManager::IdentityManager()
        :   _schema(nullptr), _reservedUpTo(0),
            _counter(0), _tableId(INVALID_TABLE_ID),
            _columnId(INVALID_COLUMN_ID), _isBootstrapped(false){}

    void IdentityManager::Bootstrap(
        const Schemas::IdentitySchema* schema,
        const Int tableId,
        const Int columnId
    ){
        if (this->_isBootstrapped.load(std::memory_order_relaxed))
            return;

        this->_schema = schema;
        this->_tableId = tableId;
        this->_columnId = columnId;

        this->_reservedUpTo.store(this->_schema->_lastValue, std::memory_order_relaxed);
        this->_counter.store(this->_schema->_lastValue, std::memory_order_relaxed);
    }

    template <DataTypes::IsInteger T>
    T IdentityManager::Generate(const ::Memory::IAllocator* allocator){
        const auto value = this->_counter.fetch_add(this->_schema->_increment, std::memory_order_relaxed);

        if (value >= this->_reservedUpTo.load(std::memory_order_relaxed))
            this->ReserveBlock(allocator, value);

        return static_cast<T>(value);
    }

    template TinyInt IdentityManager::Generate<TinyInt>(const ::Memory::IAllocator*);
    template SmallInt IdentityManager::Generate<SmallInt>(const ::Memory::IAllocator*);
    template Int IdentityManager::Generate<Int>(const ::Memory::IAllocator*);
    template BigInt IdentityManager::Generate<BigInt>(const ::Memory::IAllocator*);

    template <DataTypes::IsInteger T>
    T IdentityManager::ReserveRange(const ::Memory::IAllocator* allocator, T range){
        const auto value = this->_counter.fetch_add(range * this->_schema->_increment, std::memory_order_relaxed);

        if (value >= this->_reservedUpTo.load(std::memory_order_relaxed))
            this->ReserveBlock(allocator, value);

        return static_cast<T>(value);
    }

    template TinyInt IdentityManager::ReserveRange<TinyInt>(const ::Memory::IAllocator*, TinyInt);
    template SmallInt IdentityManager::ReserveRange<SmallInt>(const ::Memory::IAllocator*, SmallInt);
    template Int IdentityManager::ReserveRange<Int>(const ::Memory::IAllocator*, Int);
    template BigInt IdentityManager::ReserveRange<BigInt>(const ::Memory::IAllocator*, BigInt);

    template <DataTypes::IsInteger T>
    bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, T& value){
        value = this->Generate<T>(allocator);
        return true;
    }

    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, TinyInt& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, SmallInt& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, Int& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, BigInt& value);

    void IdentityManager::UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const{
        MultiThreading::WriterGuard guard(&this->mutex);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->_tableId,
            this->_columnId,
            this->_counter.load(std::memory_order_relaxed)
        );
    }

    BigInt IdentityManager::GetIncrement() const{ return this->_schema->_increment; }
}
