#include "Logger.Structures.h"

#include <cstring>


namespace DatabaseEngine::LoggingStructures {

  RowInsertBody::RowInsertBody(StorageTypes::Row *row){
   this->row = row;
  }

  void RowInsertBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
   this->row->Serialize(buffer, pos);
  }

  void RowInsertBody::Deserialize(const std::vector<char> *buffer, uint32_t &pos){
  }

  int RowInsertBody::GetSize() const{ return this->row->GetRowSize(); }

 RowUpdateBody::RowUpdateBody(StorageTypes::Row* oldRow, StorageTypes::Row* newRow){
  this->oldRow = oldRow;
  this->newRow = newRow;
 }

 void RowUpdateBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
   this->oldRow->Serialize(buffer, pos);
   this->newRow->Serialize(buffer, pos);
 }

 void RowUpdateBody::Deserialize(const std::vector<char> *buffer, uint32_t& pos){
 }

 int RowUpdateBody::GetSize() const{ return this->oldRow->GetRowSize() + this->newRow->GetRowSize(); }

 RowDeleteBody::RowDeleteBody(StorageTypes::Row *row){
   this->row = row;
 }

  void RowDeleteBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
   this->row->Serialize(buffer, pos);
  }

  void RowDeleteBody::Deserialize(const std::vector<char> *buffer, uint32_t& pos){
  }

 int RowDeleteBody::GetSize() const{ return this->row->GetRowSize(); }

 TableCreateBody::TableCreateBody() {}

 TableCreateBody::TableCreateBody(const std::string &query){
   this->query = query;
 }

 void TableCreateBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
    const uint32_t querySize = this->query.size();
    memcpy(buffer->data() + pos, &querySize, sizeof(uint32_t));
    pos += sizeof(uint32_t);

   memcpy(buffer->data() + pos, this->query.data(), querySize);
   pos += querySize;
 }

void TableCreateBody::Deserialize(const std::vector<char> *buffer, uint32_t& pos){
   uint32_t querySize = 0;

   memcpy(&querySize, buffer->data() + pos, sizeof(uint32_t));
   pos += sizeof(uint32_t);

   this->query.resize(querySize);
   memcpy(this->query.data(), buffer->data() + pos, querySize);
   pos += querySize;
}

int TableCreateBody::GetSize() const{ return sizeof(uint32_t) + this->query.size(); }
}