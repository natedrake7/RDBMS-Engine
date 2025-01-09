#pragma once
#include "../Page.h"
#include <cstdint>
#include <fstream>

using namespace std;

namespace Indexing {
class BPlusTree;
struct Node;
struct Key;
struct QueryData;
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
class Table;
}

namespace Storage {
class PageManager;
}

namespace Pages {
typedef struct IndexPageAdditionalHeader {
  page_id_t nextPageId;
  uint16_t dataSize;

  IndexPageAdditionalHeader();
  IndexPageAdditionalHeader(const page_id_t &nextPageId);
  ~IndexPageAdditionalHeader();
} IndexPageAdditionalHeader;

class IndexPage final : public Page {
	IndexPageAdditionalHeader additionalHeader;
	vector<Indexing::Node*> nodes; 
	vector<column_index_t> indexedColumns;

protected:
	void GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

	void WriteAdditionalHeaderToFile(fstream *filePtr);
  
	page_size_t CalculateTreeDataSize() const;

public:
	IndexPage(const page_id_t &pageId, const bool &isPageCreation);
	explicit IndexPage(const PageHeader &pageHeader);
	~IndexPage() override;

	void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
	void WritePageToFile(fstream *filePtr) override;

	//void WriteTreeDataToPage(Indexing::Node *node, page_offset_t &offSet);

	void SetNextPageId(const page_id_t &nextPageId);

	const page_id_t &GetNextPageId() const;

	void InsertNode(Indexing::Node*& node, page_offset_t* indexPosition);

	void DeleteNode(const page_offset_t& indexPosition);

	[[nodiscard]] Indexing::Node*& GetNodeByIndex(const page_offset_t& indexPosition);

	[[nodiscard]] Indexing::Node* GetRoot();
	};
} // namespace Pages
