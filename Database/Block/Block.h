#ifndef BLOCK_H
#define BLOCK_H

#include <iostream>
#include <ostream>
#include "../Column/Column.h"

class Column;


class Block {
    private:
        unsigned char* data;
        size_t size;
        Column* column;
    public:
        explicit Block(Column* column);

        explicit Block(const Block* block);

        Block(void* data, const size_t& size, Column* column);

        ~Block();

        void SetData(const void* inputData, const size_t& inputSize);

        void* GetBlockData() const;

        size_t& GetBlockSize();

        size_t& GetBlockIndex() const;

        size_t& GetColumnSize() const;

        void PrintBlockData() const;
};

#endif
