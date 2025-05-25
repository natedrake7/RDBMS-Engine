#pragma once
#include "../Page.h"
#include "../../B+Tree/BPlusTree.h"
#include "../../Row/Row.h"

#include <fstream>

using namespace std;

namespace Indexing {
	class BPlusTree;
	struct Node;
	struct Key;
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

	typedef struct IndexPageAdditionalHeader {
		TreeType treeType;
		page_id_t treeId;
		uint8_t numberOfSubKeys;
		bool isLeaf;
		bool isRoot;
                bool isEmpty;

		IndexPageAdditionalHeader();
		~IndexPageAdditionalHeader();
		static page_size_t GetAdditionalHeaderSize();
	}IndexPageAdditionalHeader;

	class IndexPage final : public Page {
		IndexPageAdditionalHeader additionalHeader;
		std::vector<Indexing::Key*> keys;

		//leaf
		std::vector<Indexing::BPlusTreeNonClusteredData*> nonClusteredData;

		//internal node
		std::vector<page_id_t> children;

		//only leaf
		page_id_t previousNode;
		page_id_t nextNode;

		vector<column_index_t> indexedColumns;

		protected:
			void WriteAdditionalHeaderToFile(fstream* filePtr) const;
			void ReadAdditionalHeaderFromFile(const vector<char>& data, page_offset_t &offSet);

		public:
			IndexPage(const page_id_t &pageId, const bool &isPageCreation);
			explicit IndexPage(const PageHeader &pageHeader);
			~IndexPage() override;

			void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
			void WritePageToFile(fstream *filePtr) override;

                        void MarkEmpty();

			void SetTreeType(const TreeType& treeType);
			void SetTreeId(const page_id_t& treeId);

			[[nodiscard]] const page_id_t& GetTreeId() const;

			[[nodiscard]] vector<Indexing::Key*>* GetKeysUnsafe();

			[[nodiscard]] vector<Indexing::BPlusTreeNonClusteredData*>* GetNonClusteredDataUnsafe();

			[[nodiscard]] vector<page_id_t>* GetChildren();

			void ResizeNodes(const int& splitFactor);

			[[nodiscard]] bool isEmpty() const;

			[[nodiscard]] const bool& IsLeaf() const;

			[[nodiscard]] const bool& IsRoot() const;

			void SetIsLeaf(const bool& isLeaf);

			void SetIsRoot(const bool& isRoot);

			void InsertChild(const page_id_t& child);

			void SetPreviousPage(const page_id_t& previousPage);

			void SetNextPage(const page_id_t& nextPage);

			[[nodiscard]] const page_id_t& GetPreviousPage()const;

			[[nodiscard]] const page_id_t& GetNextPage()const;

			void UpdateBytesLeft() override;

			void UpdatePageSize()override;
		};
} // namespace Pages
