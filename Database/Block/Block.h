#pragma once
#include <iostream>
#include <ostream>
#include "../Constants.h"

using namespace std;
using namespace Constants;

namespace DatabaseEngine {
    class Database;
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

        Block(const void* data, const block_size_t& size,const Column* column);

        ~Block();

        void SetData(const void* inputData, const block_size_t& inputSize);

        [[nodiscard]] object_t* GetBlockData() const;

        [[nodiscard]] block_size_t GetBlockSize() const;

        [[nodiscard]] const column_index_t& GetColumnIndex() const;

        [[nodiscard]] const row_size_t& GetColumnSize() const;

        [[nodiscard]] const ColumnType& GetColumnType() const;

        void PrintBlockData() const;
    };
}