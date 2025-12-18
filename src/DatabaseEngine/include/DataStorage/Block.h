#pragma once
#include "../PipelineConstants.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataTypes/Guid.h"
#include "../../../Systemic/include/Errors.h"

#include <cstring>

using namespace std;
using namespace Constants;

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
        void CopyToBuffer(const T& value);
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

        Block(const void* data, const block_size_t& size, const Column* column);

        Block(object_t* data, const block_size_t& size, const Column* column);

        ~Block();

        void SetData(const void* inputData, const block_size_t& inputSize);

        [[nodiscard]] Errors::RuntimeStatus SetData(const Value& value);

        [[nodiscard]] object_t* GetRawData() const;

        [[nodiscard]] block_size_t GetSize() const;

        [[nodiscard]] bool GetBool() const;
        [[nodiscard]] int8_t GetTinyInt() const;
        [[nodiscard]] int16_t GetSmallInt() const;
        [[nodiscard]] int32_t GetInt() const;
        [[nodiscard]] int64_t GetBigInt() const;
        [[nodiscard]] string GetString() const;
        [[nodiscard]] u16string GetUnicodeString() const;
        [[nodiscard]] DataTypes::Decimal GetDecimal() const;
        [[nodiscard]] DataTypes::DateTime GetDateTime() const;
        [[nodiscard]] DataTypes::Guid GetGuid() const;
        [[nodiscard]] Pages::DataObjectPointer GetLargeObjectPointer() const;
        [[nodiscard]] Pages::OverflowPointer GetOverflowPointer() const;
        
        [[nodiscard]] const column_index_t& GetColumnIndex() const;

        [[nodiscard]] const row_size_t& GetColumnSize() const;

        [[nodiscard]] const DataType& GetColumnType() const;

        [[nodiscard]] const Column* GetColumn() const;

        [[nodiscard]] bool GetIsNull() const;

        void SetColumn(const Column* otherColumn);
    };

    template <typename T> void Block::CopyToBuffer(const T &value){
        this->size = sizeof(T);
        this->data = new object_t[this->size];
        std::memcpy(this->data, &value, this->size);
    }

    void Block::CopyToBuffer(const std::string &src) {
        this->size = src.size();
        this->data = new object_t[this->size];
        std::memcpy(this->data, src.data(), this->size);
    }

    void Block::CopyToBuffer(const std::u16string &src){
        this->size = src.size();
        this->data = new object_t[this->size];
        std::memcpy(this->data, src.data(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::Decimal &src){
        this->size = src.GetRawDataSize();
        this->data = new object_t[this->size];
        std::memcpy(this->data, src.GetRawData(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::Guid &src){
        this->size = src.Size();
        this->data = new object_t[this->size];
        std::memcpy(this->data, src.GetData().data(), this->size);
    }

    void Block::CopyToBuffer(const DataTypes::DateTime &src){
        this->size = DataTypes::DateTime::DateTimeSize();
        this->data = new object_t[this->size];
        std::memcpy(this->data, &src.GetUnixTimeStamp(), this->size);
    }
}

// bool operator==(const DatabaseEngine::StorageTypes::Block& block, const Value& field);
// bool operator!=(const DatabaseEngine::StorageTypes::Block& block, const Value& field);
// bool operator>(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator<(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator>=(const DatabaseEngine::StorageTypes::Block &block, const Value &field);
// bool operator<=(const DatabaseEngine::StorageTypes::Block &block, const Value &field);