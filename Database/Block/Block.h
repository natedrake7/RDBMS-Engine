#pragma once
#include "../Constants.h"

using namespace std;
using namespace Constants;

namespace DatabaseEngine {
    class Database;
}

namespace Pages {
 struct DataObjectPointer;
}

namespace DataTypes {
    class Decimal;
}

namespace DatabaseEngine::StorageTypes {
    class Column;
    enum class ColumnType : uint8_t;
    
    class Block {
        object_t* data;
        block_size_t size;
        const Column* column;

    public:
        explicit Block(const Column* column);

        explicit Block(const Block* block);

        Block(const void* data, const block_size_t& size, const Column* column);

        ~Block();

        void SetData(const void* inputData, const block_size_t& inputSize);

        [[nodiscard]] object_t* GetBlockData() const;

        [[nodiscard]] block_size_t GetBlockSize() const;

        [[nodiscard]] bool GetBool() const;
        
        [[nodiscard]] int8_t GetTinyInt() const;

        [[nodiscard]] int16_t GetSmallInt() const;

        [[nodiscard]] int32_t GetInt() const;

        [[nodiscard]] int64_t GetBigInt() const;

        [[nodiscard]] string GetString() const;

        [[nodiscard]] u16string GetUnicodeString() const;

        [[nodiscard]] DataTypes::Decimal GetDecimal() const;

        [[nodiscard]] DataTypes::DateTime GetDateTime() const;

        [[nodiscard]] Pages::DataObjectPointer GeObjectPointer() const;
        
        [[nodiscard]] const column_index_t& GetColumnIndex() const;

        [[nodiscard]] const row_size_t& GetColumnSize() const;

        [[nodiscard]] const ColumnType& GetColumnType() const;

        void SetColumn(const Column* column);

        void PrintBlockData() const;
    };
}