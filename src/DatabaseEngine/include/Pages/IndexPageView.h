#pragma once
#include "PageView.h"
#include "Additional/IndexPageStructs.h"

namespace DataStructures{
    template<typename T>
    class Array;
}

namespace Pages{
    class IndexPageView final : public PageView {
            void InsertFirstKey(const DataTypes::Indexing::Key& key) const;
            void InsertKey(const DataTypes::Indexing::Key& key) const;

            void InsertFirstTuple(const IndexInsertTuple& tuple) const;

            DataTypes::Indexing::Key GetKeyByOffset(
                const Memory::Allocator& allocator,
                page_offset_t& offSet
            ) const;

        public:
            IndexPageView();
            explicit IndexPageView(Frame* framePtr);

            IndexPageView(IndexPageView&& other) noexcept;
            IndexPageView& operator=(IndexPageView&& other) noexcept;

            void SetTreeType(Constants::TreeType treeType) const;
            void SetTreeId(page_id_t treeId) const;
            void SetKeyTypes(const std::vector<DataType>& keyTypes) const;

            [[nodiscard]] bool IsEmpty() const;
            [[nodiscard]] bool IsLeaf() const;
            [[nodiscard]] bool IsRoot() const;
            [[nodiscard]] UnsignedTinyInt SubKeys() const;
            [[nodiscard]] UnsignedSmallInt Keys() const;

            void SetIsLeaf(bool isLeaf) const;
            void SetIsRoot(bool isRoot) const;
            void SetSubKeys(UnsignedTinyInt numberOfKeys) const;

            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key) const;
            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key, Int indexPosition) const;
            void InsertFirstChild(page_id_t child) const;

            void SetLeftSibling(page_id_t previousPage) const;
            void SetRightSibling(page_id_t nextPage) const;

            [[nodiscard]] page_id_t LeftSibling()const;
            [[nodiscard]] page_id_t RightSibling()const;

            [[nodiscard]] bool HasLeftSibling() const;
            [[nodiscard]] bool HasRightSibling() const;

            void InsertKey(const DataTypes::Indexing::Key& key, Int indexPosition) const;

            void InsertTuple(const IndexInsertTuple& tuple) const;
            void InsertTuple(const IndexInsertTuple& tuple, Int indexPosition) const;

            DataTypes::Indexing::Key GetKeyByIndex(const Memory::Allocator& allocator, Int indexPosition) const;
            LeafNodeTuple PeekLeafTuple(const Memory::Allocator& allocator,Int indexPosition) const;
            InternalNodeTuple PeekInternalNodeTuple(const Memory::Allocator& allocator, Int indexPosition) const;

            DatabaseEngine::StorageTypes::RowVersioningHeader PeekVersionHeader(const Memory::Allocator& allocator, Int indexPosition, Int& outKeySize) const;

            page_id_t GetChild(const Memory::Allocator& allocator, Int indexPosition) const;

            void AppendRowToBuffer(
                const Memory::Allocator& allocator,
                DataStructures::Array<RowReference>* buffer,
                const DatabaseEngine::Snapshot& snapshot,
                Int indexPosition
            ) const;
            void AppendRowToBuffer(
                const Memory::Allocator& allocator,
                DataStructures::Array<RowReference>* buffer,
                Int indexPosition
            ) const;

    };
}
