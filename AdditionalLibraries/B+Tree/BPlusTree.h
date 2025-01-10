#pragma once
#include <vector>
#include "../../Database/Constants.h"
#include <fstream>

using namespace std;
using namespace Constants;

namespace DatabaseEngine 
{
    class Database;
}

namespace DatabaseEngine::StorageTypes
{
    class Table;
}

namespace Pages
{
    class IndexPage;
}

namespace Indexing
{
    typedef struct BPlusTreeData
    {
        page_id_t pageId;
        extent_id_t extentId;

        BPlusTreeData();
        ~BPlusTreeData();
    }BPlusTreeData;

    typedef struct BPlusTreeNonClusteredData : public BPlusTreeData
    {
        page_offset_t index;
        BPlusTreeNonClusteredData();
        BPlusTreeNonClusteredData(const page_id_t& pageId, const extent_id_t& extentId, const page_offset_t& index);
        ~BPlusTreeNonClusteredData();
    }BPlusTreeNonClusteredData;

    typedef struct QueryData
    {
        BPlusTreeData treeData;
        int indexPosition;

        QueryData();
        QueryData(const BPlusTreeData &otherTreeData, const int &otherIndexPosition);
        ~QueryData();
    } QueryData;

    typedef struct Key
    {
        vector<object_t> value;
        key_size_t size;
        KeyType type;

        Key();
        Key(const void *keyValue, const key_size_t &keySize, const KeyType& keyType);
        ~Key();

        Key(const Key &otherKey);
        bool operator==(const Key& otherKey) const;
        bool operator>(const Key& otherKey) const;
        bool operator<(const Key& otherKey) const;
        bool operator<=(const Key& otherKey) const;
        bool operator>=(const Key& otherKey) const;
    }Key;

    typedef struct NodeHeader{
        page_id_t pageId;
        page_offset_t indexPosition;

        NodeHeader();
        ~NodeHeader();
        static page_size_t GetNodeHeaderSize();
    }NodeHeader;

    typedef struct Node
    {
        NodeHeader header;
        bool isLeaf;
        bool isRoot;
        
        vector<Key> keys;
        
        BPlusTreeData data;
        vector<BPlusTreeNonClusteredData> nonClusteredData;
        
        NodeHeader parentHeader;
        vector<NodeHeader> childrenHeaders;
        NodeHeader nextNodeHeader;
        NodeHeader previousNodeHeader;

        explicit Node(const bool &isLeaf = false, const bool& isRoot = false);
        vector<Key> &GetKeysData();
        BPlusTreeData &GetClusteredData();
        page_size_t GetNodeSize();
        ~Node();
    }Node;

    class BPlusTree final
    {
        table_id_t tableId;
        page_id_t firstIndexPageId;
        int t;
        Node *root;
        bool isDirty;
        TreeType type;
        int nonClusteredIndexId;
        DatabaseEngine::Database* database;

        void SplitChild(Node *parent, const int &index, Node *child, vector<pair<Node *, Node *>> *splitLeaves);
        void PrintTree(const Node *node, const int &level);
        Node *GetNonFullNode(Node *node, const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves);
        void DeleteNode(Node *node);
        Node *SearchKey(const Key &key) const;
        void InsertNodeToPage(Node*& node);
        Node* GetNodeFromPage(const NodeHeader& header) const;

    public:
        explicit BPlusTree(const DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Node *FindAppropriateNodeForInsert(const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves);
        void PrintTree();

        void RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void RangeQuery(const Key &minKey, const Key &maxKey, vector<BPlusTreeNonClusteredData> &result) const;
        void SearchKey(const Key &key, QueryData &result) const;
        [[nodiscard]] page_size_t GetTreeSize() const;

        Node*& GetRoot();

        void SetRoot(Node *&node);

        void SetBranchingFactor(const int &branchingFactor);

        const int &GetBranchingFactor() const;

        void WriteTreeHeaderToFile(fstream *filePtr);

        void ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

        void GetNodeSize(const Node *node, page_size_t &size) const;

        const bool& IsTreeDirty() const;

        void SetTreeType(const TreeType& treeType);

        void UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data);

        const page_id_t& GetFirstIndexPageId() const;
    };
}
