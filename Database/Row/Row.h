#ifndef ROW_H
#define ROW_H

#include <stdexcept>
#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
using namespace std;

class Row {
    private:
        size_t* maxRowSize;
        size_t* rowSize;
        vector<Block*>* data;

    protected:
        void ValidateOutOfBoundColumnHashIndex(const size_t& hashIndex) const;
        Block* GetBlock(const size_t& index) const;
        void SetRowSize(const size_t& rowSize) const;
        void SetMaxRowSize(const size_t& maxRowSize) const;

    public:
        Row();

        Row(const size_t& numberOfColumns);

        Row(const Row* row);

        ~Row();

        vector<Block*>& GetRowData() const;

        void InsertColumnData(const Block* block) const;

        void UpdateColumnData(Block* block) const;

        void DeleteColumnData(const size_t& columnHashIndex) const;

        void DeleteColumn(const size_t& columnHashIndex);
};



#endif //ROW_H
