#pragma once
#include "../../Systemic/include/Comparators.h"
#include "PageView.h"
#include "Additional/IndexPageStructs.h"

namespace Pages{
    class IndexPageView final : public PageView {
            void InsertFirstTuple(const IndexInsertTuple& tuple) const;

            DataTypes::Indexing::Key GetKeyByOffset(SlotDirectory slot) const;

            [[nodiscard]] key_size_t GetKeySize(page_offset_t offSet)const;

        public:
            IndexPageView() = default;
            explicit IndexPageView(Frame* framePtr);

            IndexPageView& operator=(const IndexPageView& other) = delete;
            IndexPageView(const IndexPageView& other) = delete;
        
            IndexPageView(IndexPageView&& other) noexcept;
            IndexPageView& operator=(IndexPageView&& other) noexcept;

            [[nodiscard]] IndexPageAdditionalHeader* GetAdditionalHeader() const;

            void SetTreeType(Constants::TreeType treeType) const;
            void SetTreeId(page_id_t treeId) const;
            void SetKeyTypes(const DataStructures::StaticArray<DataType, 10>& keyTypes) const;

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

            // void InsertKey(const DataTypes::Indexing::Key& key, Int indexPosition) const;

            void InsertTuple(const IndexInsertTuple& tuple) const;
            void InsertTuple(const IndexInsertTuple& tuple, Int indexPosition) const;

            DataTypes::Indexing::Key GetKeyByIndex(Int indexPosition) const;

            //always returns the result of the comparison of the page key against the provided key
            [[nodiscard]] Comparators::Comparator ComparePageKeyAgainst(const DataTypes::Indexing::Key& key, Int indexPosition) const;

            CoreEngine::StorageTypes::RowHeader PeekHeader(Int indexPosition) const;

            [[nodiscard]] page_id_t GetChild(Int indexPosition) const;
            InternalNodeTuple GetInternalNodeTuple( Int indexPosition) const;
            void RemoveKeyFromChild(Int indexPosition) const;
    };
}
