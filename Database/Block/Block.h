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

        object_t* GetBlockData() const;

        block_size_t GetBlockSize() const;

        const column_index_t& GetColumnIndex() const;

        const row_size_t& GetColumnSize() const;

        const ColumnType& GetColumnType() const;

        void PrintBlockData() const;
    };
}