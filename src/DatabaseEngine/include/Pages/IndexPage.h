#pragma once
#include "Page.h"
#include "../../Systemic/include/DataTypes/PackedByte.h"
#include "../DataStorage/Row.h"
#include "../DataStorage/InsertPayload.h"

namespace ByteMaps
{
	class BitMap;
}

namespace DataTypes::Indexing{
	struct Key;
}

namespace Constants{
	enum TreeType : unsigned char;
}

namespace Indexing {
	class BTree;
	struct Node;
	struct QueryData;
	struct NodeHeader;
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
	class Table;
}

namespace Storage {
	class PageManager;
}

namespace Pages {
	struct IndexPageAdditionalHeader {
		static constexpr UnsignedTinyInt TREE_TYPE_BIT_POS = 0;
		static constexpr UnsignedTinyInt IS_LEAF_BIT_POS = 2;
		static constexpr UnsignedTinyInt IS_ROOT_BIT_POS = 3;
		static constexpr UnsignedTinyInt IS_EMPTY_BIT_POS = 4;
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_POS = 5;

		static constexpr UnsignedTinyInt TREE_TYPE_BIT_MASK = 0x03;        // 0000 0011
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_MASK = 0x07; //

		std::array<DataType, Constants::MAX_NUMBER_OF_SUB_KEYS> keyTypes;
		PackedByte flags;
		page_id_t treeId;

		page_id_t previousNode;
		page_id_t nextNode;

		Constants::TreeType GetTreeType() const;
		bool IsLeaf() const;
		bool IsRoot() const;
		bool IsEmpty() const;
		UnsignedTinyInt SubKeys() const;

		// Setters
		void SetTreeType(const Constants::TreeType& type);
		void SetIsLeaf(bool value);
		void SetIsRoot(bool value);
		void SetIsEmpty(bool value);
		void SetNumberOfSubKeys(UnsignedTinyInt count);

		IndexPageAdditionalHeader();
	};

    struct IndexInsertTuple{
        DataTypes::Indexing::Key key;
        DatabaseEngine::StorageTypes::InsertPayload* payload;

        IndexInsertTuple();
        IndexInsertTuple(DataTypes::Indexing::Key& key, DatabaseEngine::StorageTypes::InsertPayload* payload);
        ~IndexInsertTuple();
    };

	struct LeafNodeTuple{
		DataTypes::Indexing::Key key;
		RowReference row;

		LeafNodeTuple& operator=(LeafNodeTuple&& other) noexcept;
		LeafNodeTuple(LeafNodeTuple&& other) noexcept;

		LeafNodeTuple& operator=(const LeafNodeTuple& other);
		LeafNodeTuple(const LeafNodeTuple& other);

		LeafNodeTuple(RowReference& row, DataTypes::Indexing::Key& key);
	};

	struct RowIdTuple{
		DataTypes::Indexing::Key key;
		DataTypes::RowIdentifier rowId;
	};

	struct InternalNodeTuple{
		DataTypes::Indexing::Key key;
		page_id_t pageId;

		InternalNodeTuple& operator=(InternalNodeTuple&& other) noexcept;
		InternalNodeTuple(InternalNodeTuple&& other) noexcept;

		InternalNodeTuple& operator=(const InternalNodeTuple& other);
		InternalNodeTuple(const InternalNodeTuple& other);

		InternalNodeTuple(DataTypes::Indexing::Key& key, page_id_t pageId);
	};

	class IndexPage final : public Page {
		IndexPageAdditionalHeader additionalHeader;
		protected:
			void WriteAdditionalHeaderToDisk(std::fstream* filePtr) const;
			void ReadAdditionalHeaderFromDisk(const std::vector<char>& data, page_offset_t &offSet);

			void InsertFirstTuple(const IndexInsertTuple& tuple);

			void InsertFirstKey(const DataTypes::Indexing::Key& key);
			void InsertKey(const DataTypes::Indexing::Key& key);

			void InsertFirstChild(page_id_t child);

			DataTypes::Indexing::Key GetKey(page_offset_t& offSet) const;

			void AdjustRows();

		public:
			IndexPage(page_id_t pageId, const DatabaseEngine::StorageTypes::Table* table, bool isPageCreation, const std::array<DataType, Constants::MAX_NUMBER_OF_SUB_KEYS>& keyTypes = {});
			explicit IndexPage(const PageHeader &pageHeader);

			void ReadFromDisk(const std::vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, std::fstream *filePtr) override;
			void WriteToDisk(std::fstream *filePtr) override;

            void MarkEmpty();

			void SetTreeType(const Constants::TreeType& treeType);
			void SetTreeId(page_id_t treeId);
			void SetKeyTypes(const std::vector<DataType>& keyTypes);

			[[nodiscard]] page_id_t GetTreeId() const;

			[[nodiscard]] bool isEmpty() const;

			[[nodiscard]] bool IsLeaf() const;

			[[nodiscard]] bool IsRoot() const;

			[[nodiscard]] UnsignedTinyInt SubKeys() const;
			void SetSubKeys(UnsignedTinyInt numberOfKeys);

			void SetIsLeaf(bool isLeaf);
			void SetIsRoot(bool isRoot);


			void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key);
			void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key, Int indexPosition);
			void InsertChild(page_id_t child);

			void SetPreviousPage(page_id_t previousPage);
			void SetNextPage(page_id_t nextPage);

			[[nodiscard]] page_id_t GetPreviousPage()const;
			[[nodiscard]] page_id_t GetNextPage()const;

			[[nodiscard]] bool HasRightSibling() const;
			[[nodiscard]] bool HasLeftSibling() const;

			void InsertKey(const DataTypes::Indexing::Key& key, Int indexPosition);

			void InsertTuple(const IndexInsertTuple& tuple);
			void InsertTuple(const IndexInsertTuple& tuple, Int indexPosition);

			DataTypes::Indexing::Key GetKey(Int indexPosition) const;
			DatabaseEngine::StorageTypes::Row GetRow(Int indexPosition, Int offSet, const DatabaseEngine::StorageTypes::Table* table) const;
			LeafNodeTuple PeekLeafTuple(Int indexPosition);
			InternalNodeTuple PeekInternalNodeTuple(Int indexPosition) const;

			DatabaseEngine::StorageTypes::RowVersioningHeader PeekVersionHeader(Int indexPosition, Int& outKeySize) const;

			page_id_t GetChild(Int indexPosition) const;

			void UpdateBytesLeft() override;

			void UpdatePageSize()override;

			[[nodiscard]] Int NumberOfKeys()const;

			void AppendRowToBuffer(
				std::vector<RowReference>* buffer,
				const DatabaseEngine::StorageTypes::Table* table,
				const DatabaseEngine::Snapshot& snapshot,
				Int indexPosition
			);
		};
} // namespace Pages
