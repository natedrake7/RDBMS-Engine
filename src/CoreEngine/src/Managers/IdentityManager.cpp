#include <CoreEngine/Managers/IdentityManager.h>

#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <Server/Server.h>
#include <Systemic/Guards/WriterGuard.h>

namespace CoreEngine::StorageTypes {
    void IdentityManager::ReserveBlock(const ::Memory::IAllocator* allocator, const Int64_t value){
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
        const Int32_t tableId,
        const Int32_t columnId
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

    template Int8_t IdentityManager::Generate<Int8_t>(const ::Memory::IAllocator*);
    template Int16_t IdentityManager::Generate<Int16_t>(const ::Memory::IAllocator*);
    template Int32_t IdentityManager::Generate<Int32_t>(const ::Memory::IAllocator*);
    template Int64_t IdentityManager::Generate<Int64_t>(const ::Memory::IAllocator*);

    template <DataTypes::IsInteger T>
    T IdentityManager::ReserveRange(const ::Memory::IAllocator* allocator, T range){
        const auto value = this->_counter.fetch_add(range * this->_schema->_increment, std::memory_order_relaxed);

        if (value >= this->_reservedUpTo.load(std::memory_order_relaxed))
            this->ReserveBlock(allocator, value);

        return static_cast<T>(value);
    }

    template Int8_t IdentityManager::ReserveRange<Int8_t>(const ::Memory::IAllocator*, Int8_t);
    template Int16_t IdentityManager::ReserveRange<Int16_t>(const ::Memory::IAllocator*, Int16_t);
    template Int32_t IdentityManager::ReserveRange<Int32_t>(const ::Memory::IAllocator*, Int32_t);
    template Int64_t IdentityManager::ReserveRange<Int64_t>(const ::Memory::IAllocator*, Int64_t);

    template <DataTypes::IsInteger T>
    bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, T& value){
        value = this->Generate<T>(allocator);
        return true;
    }

    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, Int8_t& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, Int16_t& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, Int32_t& value);
    template bool IdentityManager::TryGenerate(const ::Memory::IAllocator* allocator, Int64_t& value);

    void IdentityManager::UpdateMasterDbOnShutdown(const ::Memory::IAllocator* allocator) const{
        MultiThreading::WriterGuard guard(&this->mutex);

        SystemCatalog::Get().UpdateIdentityByColumnId(
            allocator,
            this->_tableId,
            this->_columnId,
            this->_counter.load(std::memory_order_relaxed)
        );
    }

    Int64_t IdentityManager::GetIncrement() const{ return this->_schema->_increment; }

    void IdentityManager::Reseed(const Int64_t value){
        MultiThreading::WriterGuard guard(&this->mutex);
        this->_reservedUpTo.store(value, std::memory_order_relaxed);
        this->_counter.store(value, std::memory_order_relaxed);
    }
}
