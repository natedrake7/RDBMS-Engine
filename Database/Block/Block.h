#ifndef BLOCK_H
#define BLOCK_H

#include <iostream>
#include <ostream>

#include "../Database.h"
#include "../Column/Column.h"

class Database;
class Column;

class Block {
    object_t* data;
    block_size_t size;
    bool isLargeObject;
    const Column* column;

    public:
        explicit Block(const Column* column);

        explicit Block(const Block* block);

        Block(const void* data, const block_size_t& size,const Column* column);

        ~Block();

        void SetData(const void* inputData, const block_size_t& inputSize, const bool& isLargeObject = false);

        object_t* GetBlockData() const;

        const block_size_t& GetBlockSize() const;

        const column_index_t& GetColumnIndex() const;

        const record_size_t& GetColumnSize() const;

        const ColumnType& GetColumnType() const;

        void PrintBlockData(const Database* db) const;

        const bool& IsLargeObject() const;

        void SetIsLargeObject(const bool& isLargeObject);
};

#endif
