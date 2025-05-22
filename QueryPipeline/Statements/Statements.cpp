#include "Statements.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

namespace QueryPipeline::Statements {
  Expression Expression::Predicate(const std::string &column, const std::string &operation, const Field &value) {
    return Expression{
      ExpressionType::Predicate,
      nullptr,
      nullptr,
      column,
      operation,
      value
    };
  }

  Expression Expression::Logical(const ExpressionType &type, Expression *leftExpression, Expression *RightExpression){
    return Expression{
      type,
      leftExpression,
      RightExpression
    };
  }

  Expression::~Expression(){
    delete left;
    delete right;
  }

  void Expression::Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary){
    if (this->type != Statements::ExpressionType::Predicate
      && this->left != nullptr
      && this->right != nullptr) {
       this->left->Validate(columnsDictionary);
        this->right->Validate(columnsDictionary);

      return;
    }

    Headers::ColumnHeader header;
    if (!columnsDictionary.TryGetValue(this->column, header))
      throw runtime_error("Column " + this->column + " does not exist");

    this->columnIndex = header.tablePosition;  
  }

  bool Expression::IsComplex()const{
    if (this->left != nullptr && this->right != nullptr)
      return this->left->IsComplex() || this->right->IsComplex();

    if (this->type == ExpressionType::Or)
      return true;

    return false;
  }

  void Expression::GetColumns(HashSet<column_index_t>& columnsSet) const{
    if (this->left != nullptr && this->right != nullptr) {
        this->left->GetColumns(columnsSet);
        this->right->GetColumns(columnsSet);
    }

    if (!columnsSet.contains(this->columnIndex))
      columnsSet.Add(this->columnIndex);
  }

  void CreateTableStatement::Validate(){
    const std::string temp = "MoviesDb";

    if (!Server::ServerInstance::Get().DatabaseExists(temp))
      throw runtime_error("No Database with name: "  + temp + " exists");
    
    const auto tables = Server::ServerInstance::Get().SelectTables(temp);


    const Headers::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == this->name) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr != nullptr)
      throw runtime_error("Table " + this->name + " already exists");
    
    column_index_t tablePosition = 0;
    for (auto& column: this->columns) {
      uint16_t columnSize;
      if (!ColumnTypeSizes.TryGetValue(column.type.name, columnSize))
        throw runtime_error("Column Type: " + column.type.name + " does not exist");

      if (columnSize != 0) {
        column.type.size = columnSize;
      }

      if (column.type.beforeFraction != 0 || column.type.afterFraction != 0) {
        //decimal handle
      }

      column.index = tablePosition++;
      if (column.isPrimaryKey)
        this->primaryKey.push_back(column.index);
    }
  }

  LogicalPlan * CreateTableStatement::ToLogical(){
    return new LogicalTableCreate(this->dbName, this->name, this->columns, this->primaryKey);
  }

  void SelectStatement::Validate(){
    const auto tables = Server::ServerInstance::Get().SelectTables(this->dbName);

    const Headers::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == this->table) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr == nullptr)
      throw runtime_error("Table " + this->table + " does not exist");

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->dbName, headerPtr->name);

    if (!this->columns.empty() && this->columns[0] == "*") {
      for (const auto& [key, header] : columnsDict) {
        this->columnIndices.emplace_back(header.tablePosition);
      }
    }
    else {
      for (const auto& selectColumn : this->columns) {
        if (Headers::ColumnHeader header ;columnsDict.TryGetValue(selectColumn, header)) {
          this->columnIndices.emplace_back(header.tablePosition);
          continue;
        }
      
        throw runtime_error("Column " + selectColumn + " does not exist");
      }
    }

    if (this->where.expression == nullptr)
      return;

    this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * SelectStatement::ToLogical(){
    const auto scanTable = new LogicalTableScan(this->dbName, this->table, this->where.expression);

    LogicalPlan* current = scanTable;

    if (this->where.expression != nullptr)
      current = new LogicalFilter(this->dbName, current, this->where.expression);
    

    if (!this->columns.empty())
      current = new LogicalProject(this->dbName, current, this->columnIndices);

    return current;
  }

  void CreateDbStatement::Validate(){
    if (!Server::ServerInstance::Get().DatabaseExists(this->name))
      return;

    throw runtime_error("Database " + this->name + " already exists");
  }

  LogicalPlan * CreateDbStatement::ToLogical(){
    return new LogicalCreateDatabase(this->name);
  }

  void DropDbStatement::Validate(){
    const auto database = Server::ServerInstance::Get().SelectDatabases(this->name);

    if (database.name.empty())
      throw runtime_error("Cannot drop: " + this->name + ". Database" + this->name + " does not exist");

    if (database.isSystem)
      throw runtime_error("Cannot drop: a system database");
  }
  
  LogicalPlan * DropDbStatement::ToLogical(){
    return nullptr;
  }

  void InsertStatement::Validate(){
    const auto tables = Server::ServerInstance::Get().SelectTables(this->dbName);

    const Headers::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == this->tableName) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr == nullptr)
      throw runtime_error("Table " + this->tableName + " does not exist");

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->dbName, headerPtr->name);

    if (this->columns.size() != this->values.size())
      throw runtime_error("Invalid number of arguments supplied");

    for (int i = 0;i < this->columns.size(); i++) {
      const auto& column = this->columns[i];

      Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(column, header))
        throw runtime_error("Column " + column + " does not exist on table: " + headerPtr->name);

      this->values.at(i).Validate(header);
    }

    for (const auto&[columnName, header]:  columnsDict) {
      if (header.isSystem)
        continue;
      
      bool columnExistsInStatement = false;
      
      for (const auto& statementColumn: this->columns) {
        if (columnName != statementColumn)
          continue;
          
        columnExistsInStatement = true;
        break;
      }

      if (!columnExistsInStatement && !header.isNullable)
        throw invalid_argument("Column " + columnName + " does not allow NULLS. Insert fails");

      if (columnExistsInStatement)
        continue;

      this->values.emplace_back(nullptr, header.tablePosition);
    }
  }

  LogicalPlan* InsertStatement::ToLogical() {
    return new QueryPipeline::LogicalInsert(this->dbName, this->tableName, this->values);
  }


}
