#pragma once
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataTypes/Guid.h"
#include "../../../Systemic/include/Errors.h"
#include <cstring>

namespace DatabaseEngine {
    class Database;
}

namespace Pages {
 struct DataObjectPointer;
 struct OverflowPointer;
}

namespace DataTypes {
    class Decimal;
}

namespace DatabaseEngine::StorageTypes {
    class Column;
    
    class Block {
        object_t* data;
        block_size_t size;
        const Column* column;

        Errors::RuntimeStatus SetDataByType(const Value& value);

        template <typename T>
        void CopyToBuffer(T value);
        inline void CopyToBuffer(const std::string& src);
        inline void CopyToBuffer(const std::u16string& src);
        inline void CopyToBuffer(const DataTypes::Decimal& src);
        inline void CopyToBuffer(const DataTypes::DateTime& src);
        inline void CopyToBuffer(const DataTypes::Guid& src);

        inline Errors::RuntimeStatus SetTinyInt(const Value& value);
        inline Errors::RuntimeStatus SetSmallInt(const Value& value);
        inline Errors::RuntimeStatus SetInt(const Value& value);
        inline Errors::RuntimeStatus SetBigInt(const Value& value);
        inline Errors::RuntimeStatus SetDecimal(const Value& value);
        inline Errors::RuntimeStatus SetString(const Value& value);
        inline Errors::RuntimeStatus SetUnicodeString(const Value& value);
        inline Errors::RuntimeStatus SetBool(const Value& value);
        inline Errors::RuntimeStatus SetDateTime(const Value& value);
        inline Errors::RuntimeStatus SetGuid(const Value& value);


    public:
        explicit Block();
        explicit Block(const Column* column);
        explicit Block(const Block* block);

        Block(const void* data, block_size_t size, const Column* column);
        Block(object_t* data, block_size_t size, const Column* column);

        ~Block();

        void SetData(const void* inputData, block_size_t inputSize);
        [[nodiscard]] Errors::RuntimeStatus SetData(const Value& value);

        [[nodiscard]] object_t* Data() const;
        [[nodiscard]] block_size_t Size() const;

        [[nodiscard]] bool AsBool() const;
        [[nodiscard]] TinyInt AsTinyInt() const;
        [[nodiscard]] SmallInt AsSmallInt() const;
        [[nodiscard]] Int AsInt() const;
        [[nodiscard]] BigInt AsBigInt() const;
        [[nodiscard]] std::string AsString() const;
        [[nodiscard]] std::u16string AsUnicodeString() const;
        [[nodiscard]] DataTypes::Decimal AsDecimal() const;
        [[nodiscard]] DataTypes::DateTime AsDateTime() const;
        [[nodiscard]] DataTypes::Guid AsGuid() const;
        [[nodiscard]] Pages::DataObjectPointer AsLargeObjectPointer() const;
        [[nodiscard]] Pages::OverflowPointer AsOverflowPointer() const;
        
        [[nodiscard]] column_index_t ColumnIndex() const;
        [[nodiscard]] row_size_t ColumnSize() const;
        [[nodiscard]] DataType ColumnType() const;
        [[nodiscard]] bool Null() const;

        [[nodiscard]] const Column* GetColumn() const;
        void SetColumn(const Column* otherColumn);
    };

    template <typename T> void Block::CopyToBuffer(T value){
        this->size = sizeof(T);
        this->data = static_cast<object_t*>(std::malloc(this->size));
        std::memcpy(this->data, &value, this->size);
    }

    void Block::CopyToBuffer(const std::string &src) {
        this->size = src.size();
        this->data = static_cast<object_t*>(std::malloc(this->size));
        std::memcpy(this->data, src.data(), this->size);
    }

    void Block::CopyToBuffer(const std::u16string &src){
        this->size = src.size();
        this->data = static_cast<object_t*>(std::malloc(this->size));
        std::memcpy(this->data, src.data(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::Decimal &src){
        this->size = src.GetRawDataSize();
        this->data = static_cast<object_t*>(std::malloc(this->size));
        std::memcpy(this->data, src.GetRawData(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::Guid &src){
        this->size = DataTypes::Guid::Size();
        this->data = static_cast<object_t*>(std::malloc(this->size));
        std::memcpy(this->data, src.GetData().data(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::DateTime &src){
        this->size = DataTypes::DateTime::Size();
        this->data = static_cast<object_t*>(std::malloc(this->size));

        const auto dt = src.GetUnixTimeStamp();
        std::memcpy(this->data, &dt, this->size);
    }
}

// bool operator==(const DatabaseEngine::StorageTypes::Block& block, const Value& field);
// bool operator!=(const DatabaseEngine::StorageTypes::Block& block, const Value& field);
// bool operator>(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator<(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator>=(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator<=(const DatabaseEngine::StorageTypes::Block &block, const Value &field);