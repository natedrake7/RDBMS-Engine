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
    typedef struct BPlusTreeNonClusteredData
    {
        page_id_t pageId;
        page_offset_t index;
        BPlusTreeNonClusteredData();
        BPlusTreeNonClusteredData(const page_id_t& pageId, const page_offset_t& index);
        ~BPlusTreeNonClusteredData();

        static page_size_t GetNonClusteredDataSize();
    }BPlusTreeNonClusteredData;

    typedef struct QueryData
    {
        page_id_t pageId;
        page_offset_t indexPosition;

        QueryData();
        QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition);
        ~QueryData();
    } QueryData;

    typedef struct Key
    {
        vector<object_t> value;
        key_size_t size;
        KeyType type;
        vector<Key> subKeys;

        Key();
        Key(const void *keyValue, const key_size_t &keySize, const KeyType& keyType);

        explicit Key(const vector<Key>& subKeys);
        ~Key();

        Key(const Key &otherKey);
        bool operator==(const Key& otherKey) const;
        bool operator>(const Key& otherKey) const;
        bool operator<(const Key& otherKey) const;
        bool operator<=(const Key& otherKey) const;
        bool operator>=(const Key& otherKey) const;

        [[nodiscard]] int GetKeySize() const;
        [[nodiscard]] int CompareCompositeKeys(const Key& otherKey) const;
        void InsertKey(const Key &otherKey);
    }Key;

    typedef struct NodeHeader{
        page_id_t pageId;
        page_offset_t indexPosition;

        NodeHeader();
        NodeHeader(const page_id_t& pageId, const page_offset_t& indexPosition);
        NodeHeader(const NodeHeader& otherHeader);
        ~NodeHeader();
        static page_size_t GetNodeHeaderSize();
    }NodeHeader;

    typedef struct Node
    {
        NodeHeader header;
        bool isLeaf;
        bool isRoot;
        bool isNodeClustered;
        
        vector<Key> keys;
        
        page_id_t dataPageId;
        vector<BPlusTreeNonClusteredData> nonClusteredData;
        
        NodeHeader parentHeader;
        vector<NodeHeader> childrenHeaders;
        NodeHeader nextNodeHeader;
        NodeHeader previousNodeHeader;

        page_size_t prevNodeSize;
        page_size_t currentNodeSize;

        explicit Node(const bool &isLeaf = false, const bool& isRoot = false, const bool& isNodeClustered = false);
        [[nodiscard]] page_size_t GetNodeSize() const;
        ~Node();
    }Node;

    class BPlusTree final
    {
        table_id_t tableId;
        page_id_t firstIndexPageId;
        int t;
        Node *root;
        TreeType type;
        int nonClusteredIndexId;
        DatabaseEngine::Database* database;

        void SplitChild(Node *parent, const int &index, Node *child);
        void PrintTree(const Node *node, const int &level);
        Node *GetNonFullNode(Node *node, const Key &key, int *indexPosition);
        void DeleteNode(const Node *node);
        [[nodiscard]] Node *SearchKey(const Key &key) const;
        void InsertNodeToPage(Node*& node, const page_id_t& parentPageId);

        static Node* GetNodeFromPage(const NodeHeader& header);
        static int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* table, const TreeType& treeType, const int& nonClusteredIndexId);

    public:
        explicit BPlusTree(const DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Node *FindAppropriateNodeForInsert(const Key &key, int *indexPosition);
        void PrintTree();

        void RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void RangeQuery(const Key &minKey, const Key &maxKey, vector<BPlusTreeNonClusteredData> &result) const;
        void SearchKey(const Key &key, QueryData &result) const;
        [[nodiscard]] page_size_t GetTreeSize() const;

        Node*& GetRoot();

        void SetRoot(Node *&node);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void WriteTreeHeaderToFile(fstream *filePtr) const;

        void ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

        void GetNodeSize(const Node *node, page_size_t &size) const;

        void SetTreeType(const TreeType& treeType);

        void UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data) const;

        [[nodiscard]] const page_id_t& GetFirstIndexPageId() const;
    };
}
