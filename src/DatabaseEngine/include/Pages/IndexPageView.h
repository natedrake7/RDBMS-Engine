#pragma once
#include "IndexPage.h"
#include "PageView.h"


namespace Pages{
    struct IndexInsertTuple;
    struct IndexPageAdditionalHeader;

    class IndexPageView final : public PageView {
        IndexPageAdditionalHeader* additionalHeaderPtr;

            void InsertFirstChild(page_id_t child) const;

            void InsertFirstKey(const DataTypes::Indexing::Key& key) const;
            void InsertKey(const DataTypes::Indexing::Key& key) const;

            void InsertFirstTuple(const IndexInsertTuple& tuple) const;

            DataTypes::Indexing::Key GetKey(page_offset_t& offSet) const;

        public:
            explicit IndexPageView(Frame* framePtr);

            void SetTreeType(TreeType treeType) const;
            void SetTreeId(page_id_t treeId) const;
            void SetKeyTypes(const std::vector<DataType>& keyTypes) const;

            [[nodiscard]] bool IsEmpty() const;
            [[nodiscard]] bool IsLeaf() const;
            [[nodiscard]] bool IsRoot() const;
            [[nodiscard]] UnsignedTinyInt SubKeys() const;

            void SetIsLeaf(bool isLeaf) const;
            void SetIsRoot(bool isRoot) const;
            void SetSubKeys(UnsignedTinyInt numberOfKeys) const;

            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key) const;
            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key, Int indexPosition) const;
            void InsertChild(page_id_t child) const;

            void SetLeftSibling(page_id_t previousPage) const;
            void SetRightSibling(page_id_t nextPage) const;

            [[nodiscard]] page_id_t LeftSibling()const;
            [[nodiscard]] page_id_t RightSibling()const;

            [[nodiscard]] bool HasLeftSibling() const;
            [[nodiscard]] bool HasRightSibling() const;

            void InsertKey(const DataTypes::Indexing::Key& key, Int indexPosition) const;

            void InsertTuple(const IndexInsertTuple& tuple) const;
            void InsertTuple(const IndexInsertTuple& tuple, Int indexPosition) const;

            DataTypes::Indexing::Key GetKey(Int indexPosition) const;
            LeafNodeTuple PeekLeafTuple(Int indexPosition) const;
            InternalNodeTuple PeekInternalNodeTuple(Int indexPosition) const;

            DatabaseEngine::StorageTypes::RowVersioningHeader PeekVersionHeader(Int indexPosition, Int& outKeySize) const;

            page_id_t GetChild(Int indexPosition) const;

            void AppendRowToBuffer(
                std::vector<RowReference>* buffer,
                const DatabaseEngine::StorageTypes::Table* table,
                const DatabaseEngine::Snapshot& snapshot,
                Int indexPosition
            );
    };
}
