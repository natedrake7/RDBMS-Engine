#include "../../include/Logger/Logger.Structures.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"
#include "../../include/DataStorage/Table.h"

#include <cstring>

namespace DatabaseEngine::LoggingStructures {

  std::ostream& operator<<(std::ostream& os, const LogEntryBody& logEntry){
    return logEntry.Print(os);
  }

   RowInsertBody::RowInsertBody(){}

  RowInsertBody::RowInsertBody(const Pointer<StorageTypes::Row>& row){
   this->row = row;
  }

  void RowInsertBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
   this->row->Serialize(buffer, pos);
  }

  void RowInsertBody::Deserialize(
    const std::vector<char> *buffer,
    uint32_t &pos,
    const StorageTypes::Table* table){
    this->row = Pointer(new StorageTypes::Row(*table));

    this->row->Deserialize(buffer, pos);
  }

  int RowInsertBody::GetSize() const{ return static_cast<int>(this->row->TotalSize()); }

  std::ostream& RowInsertBody::Print(std::ostream& os)const{
    os << *this->row << std::endl;
    return os;
  }

  BatchRowInsertBody::BatchRowInsertBody(){
    this->rows = nullptr;
  }

  BatchRowInsertBody::BatchRowInsertBody(const std::vector<Pointer<StorageTypes::Row>>& rows){
    this->rows = &rows;
  }

  void BatchRowInsertBody::Serialize(std::vector<char>* buffer, uint32_t& pos){
    for (const auto& row : *this->rows){
      row->Serialize(buffer, pos);
    }
  }

  const Pointer<StorageTypes::Row>& RowInsertBody::GetLastRowStatus() const{ return this->row; }

  RowUpdateBody::RowUpdateBody(){
    this->oldRow = nullptr;
    this->newRow = nullptr;
  }

  RowUpdateBody::RowUpdateBody(StorageTypes::Row* oldRow, StorageTypes::Row* newRow){
    this->oldRow = oldRow;
    this->newRow = newRow;
   }

   void RowUpdateBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
     this->oldRow->Serialize(buffer, pos);
     this->newRow->Serialize(buffer, pos);
   }

   void RowUpdateBody::Deserialize(
     const std::vector<char> *buffer,
     uint32_t& pos,
     const StorageTypes::Table* table){
      this->oldRow = new StorageTypes::Row(*table);

      this->oldRow->Deserialize(buffer, pos);

      this->newRow = new StorageTypes::Row(*table);

      this->newRow->Deserialize(buffer, pos);
   }

   int RowUpdateBody::GetSize() const{ return static_cast<int>(this->oldRow->TotalSize() + this->newRow->TotalSize()); }

    std::ostream & RowUpdateBody::Print(std::ostream &os) const{
        os << *this->oldRow << std::endl;
        os << *this->newRow << std::endl;

        return os;
    }

    const Pointer<StorageTypes::Row>& RowUpdateBody::GetLastRowStatus() const{ return {}; }

    RowDeleteBody::RowDeleteBody(){
       this->row = nullptr;
    }

    RowDeleteBody::RowDeleteBody(StorageTypes::Row *row){
     this->row = row;
   }

    void RowDeleteBody::Serialize(std::vector<char> *buffer, uint32_t &pos){
     this->row->Serialize(buffer, pos);
    }

    void RowDeleteBody::Deserialize(
      const std::vector<char> *buffer,
      uint32_t& pos,
      const StorageTypes::Table* table){
      this->row = new StorageTypes::Row(*table);

      this->row->Deserialize(buffer, pos);
    }

   int RowDeleteBody::GetSize() const{ return static_cast<int>(this->row->TotalSize()); }

    std::ostream & RowDeleteBody::Print(std::ostream &os) const{
      os << *this->row << std::endl;
      return os;
    }

    const Pointer<StorageTypes::Row>& RowDeleteBody::GetLastRowStatus() const{ return {}; }

   TableCreateBody::TableCreateBody() = default;

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

  void TableCreateBody::Deserialize(const std::vector<char> *buffer, uint32_t& pos, const StorageTypes::Table* table){
     uint32_t querySize = 0;

     memcpy(&querySize, buffer->data() + pos, sizeof(uint32_t));
     pos += sizeof(uint32_t);

     this->query.resize(querySize);
     memcpy(this->query.data(), buffer->data() + pos, querySize);
     pos += querySize;
  }

  int TableCreateBody::GetSize() const{ return static_cast<int>(sizeof(uint32_t) + this->query.size()); }

  std::ostream & TableCreateBody::Print(std::ostream &os) const{
      os << this->query << std::endl;
      return os;
  }

  const Pointer<StorageTypes::Row>& TableCreateBody::GetLastRowStatus() const{ return {}; }
}