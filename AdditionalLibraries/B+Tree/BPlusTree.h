#pragma once
#include <vector>
#include "../../Database/Constants.h"

using namespace std;
using namespace Constants;

class Table;

namespace Indexing {

    typedef struct BPlusTreeData {
        page_id_t pageId;
        page_offset_t rowIndex;

        BPlusTreeData();
        BPlusTreeData(const page_id_t& pageId, const page_offset_t& rowIndex);
        ~BPlusTreeData();
    }BPlusTreeData;

    class BPlusTree final{
    public:
        struct Node {

            bool isLeaf;
            vector<int> keys;
            vector<BPlusTreeData> data;
            vector<Node*> children;
            Node* next;
            Node* prev;

            explicit Node(const bool& isLeaf = false);
        };

    private:    

        table_id_t tableId;
        int t;
        Node* root;

        void SplitChild(Node* parent,const int& index, Node* child) const;
        void PrintTree(Node* node, const int& level);
        void InsertToNonFullNode(Node* node, const int& key, const BPlusTreeData& value);
        void DeleteNode(Node* node);
        Node* SearchKey(const int &key) const;
        void GetNodeSize(Node* node, page_size_t& size) const;


    public:
        explicit BPlusTree(const Table* table);
        ~BPlusTree();

        void Insert(const int& key, const BPlusTreeData& value);
        void PrintTree();

        void RangeQuery(const int& minKey, const int& maxKey, vector<BPlusTreeData>& result) const;
        void SearchKey(const int& key, BPlusTreeData& result) const;
        page_size_t GetTreeSize() const;

        void SetBranchingFactor(const int& branchingFactor);
    };
}

