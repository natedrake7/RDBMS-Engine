#ifndef ROW_H
#define ROW_H
#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
class Table;

using namespace std;

class Row {
    private:
        size_t* rowSize;
        vector<Block*> data;
        const Table* table;
    
    protected:
        void ValidateOutOfBoundColumnIndex(const size_t& columnIndex) const;
        void SetRowSize(const size_t& rowSize) const;

    public:
        explicit Row(const Table& table);

        ~Row();

        vector<Block*>& GetRowData();

        void InsertColumnData(Block* block);

        void UpdateColumnData(Block* block);

        void DeleteColumnData(const size_t& columnIndex);

        void DeleteColumn(const size_t& columnIndex);

        const Block* GetBlock(const size_t& index) const;
};



#endif //ROW_H
