﻿#ifndef BLOCK_H
#define BLOCK_H

#include <iostream>
#include <ostream>

#include "../Database.h"
#include "../Column/Column.h"

class Database;
class Column;


class Block {
    private:
        unsigned char* data;
        size_t* size;
        const Column* column;

    public:
        explicit Block(const Column* column);

        // explicit Block(const Block* block);

        Block(const void* data, const size_t& size,const Column* column);

        ~Block();

        void SetData(const void* inputData, const size_t& inputSize);

        unsigned char* GetBlockData() const;

        size_t& GetBlockSize() const;

        const size_t& GetColumnIndex() const;

        const size_t& GetColumnSize() const;

        const ColumnType& GetColumnType() const;

        void PrintBlockData(const Database* db) const;
};

#endif
