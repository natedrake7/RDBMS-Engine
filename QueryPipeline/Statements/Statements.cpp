#include "Statements.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

#include <iostream>

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

  bool Expression::Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary){
    if (this->type != Statements::ExpressionType::Predicate
      && this->left != nullptr
      && this->right != nullptr)
       return this->left->Validate(columnsDictionary) &&
              this->right->Validate(columnsDictionary);

    Headers::ColumnHeader header;
    if (!columnsDictionary.TryGetValue(this->column, header))
      return false;

    this->columnIndex = header.tablePosition;

    return true;
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

  bool DeleteStatement::Validate(){
    if (!Server::ServerInstance::Get().TableExists(this->dbName, this->table->name, this->table->schema))
      return false;

    if (this->where.expression == nullptr)
      return true;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->dbName, this->table->name);

    return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->dbName, this->table, this->where.expression);
  }

  CreateTableStatement::~CreateTableStatement() {
      delete this->constraint;
      delete this->table;
  }

  bool CreateTableStatement::Validate(){
    //no need to check as the below query will just return 0 results
    // if (!Server::ServerInstance::Get().DatabaseExists(this->dbName))
    //   throw runtime_error("No Database with name: "  + this->dbName + " exists");
    
    if (Server::ServerInstance::Get().TableExists(this->dbName, this->table->name, this->table->schema))
      throw runtime_error("Table " + this->table->name + " already exists");
    
    column_index_t tablePosition = 0;
    bool primaryKeyFound = false;
    Dictionary<string, column_index_t> columnNamesToIndexes;
    
    for (auto& column: this->columns) {
      uint16_t columnSize;
      
      if (!ColumnTypeSizes.TryGetValue(column.type.name, columnSize)) {
        std::cerr << "Column Type: " + column.type.name + " does not exist" << endl;
        return false;
      }

      if (columnSize != 0)
        column.type.size = columnSize;

      if (column.type.beforeFraction != 0 || column.type.afterFraction != 0) {
        //decimal handle
      }

      column.index = tablePosition++;

      columnNamesToIndexes.Add(column.name, column.index);
      
      if (column.isPrimaryKey) {
        this->primaryKey.push_back(column.index);
        primaryKeyFound = true;
      }
    }

    if (this->constraint == nullptr)
      return true;

    if (primaryKeyFound) {
      cerr << "Cannot have a primary key and a constraint declared" << endl;
      return false;
    }

    //primary key will be clear for sure here
    for (const auto& column: this->constraint->columns)
      this->primaryKey.push_back(columnNamesToIndexes[column]);

    return true;
  }

  LogicalPlan * CreateTableStatement::ToLogical(){
    const auto constraintName = this->constraint == nullptr ? "" : this->constraint->name;
    
    return new LogicalTableCreate(this->dbName, this->table, this->columns, this->primaryKey, constraintName);
  }

  bool SelectStatement::Validate(){
    if (!Server::ServerInstance::Get().TableExists(this->dbName, this->table->name, this->table->schema)) {
      cerr << "Table " + this->table->schema + "." + this->table->name + " does not exist" << endl;
      return false;
    }

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->dbName, this->table->name);

    if (!this->columns.empty() && this->columns[0] == "*") {
      for (const auto& [key, header] : columnsDict)
        this->columnIndices.emplace_back(header.tablePosition);
    }
    else {
      for (const auto& selectColumn : this->columns) {
        if (Headers::ColumnHeader header ;columnsDict.TryGetValue(selectColumn, header)) {
          this->columnIndices.emplace_back(header.tablePosition);
          continue;
        }

        cerr << "Column " + selectColumn + " does not exist" << endl;
      }
    }

    if (this->where.expression == nullptr)
      return true;

    return this->where.expression->Validate(columnsDict);
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

  bool CreateDbStatement::Validate(){
    if (!Server::ServerInstance::Get().DatabaseExists(this->name))
      return true;

    cerr << "Database " + this->name + " already exists" << endl;
    return false;
  }

  LogicalPlan * CreateDbStatement::ToLogical(){
    return new LogicalCreateDatabase(this->name);
  }

  bool DropDbStatement::Validate(){
    const auto database = Server::ServerInstance::Get().SelectDatabase(this->name);

    if (database.name.empty()) {
      cerr << "Cannot drop: " << this->name << ". Database" << this->name << " does not exist" << endl;
      return false;
    }

    if (database.isSystem) {
      cerr << "Cannot drop: " << this->name << ". Database" << this->name << " is a system database" << endl;
     return false;
    }

    return true;
  }
  
  LogicalPlan * DropDbStatement::ToLogical(){
    return nullptr;
  }

  bool InsertStatement::Validate(){
    if (!Server::ServerInstance::Get().TableExists(this->dbName, this->table->name, this->table->schema)) {

      cerr << "Table " << this->table->schema << "." + this->table->name << " does not exist" << endl;
      return false;
    }
    
    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->dbName, this->table->name);

    if (this->columns.size() != this->values.size()) {
      cerr << "Invalid number of arguments supplied" << endl;
      return false;
    }

    for (int i = 0;i < this->columns.size(); i++) {
      const auto& column = this->columns[i];

      Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(column, header)) {
        cerr << "Column " << column << " does not exist on table: " << this->table->name << endl;
        return false;
      }

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

      if (!columnExistsInStatement && !header.isNullable) {
        cerr << "Column " << columnName << " does not allow NULLS. Insert fails";
        return false;
      }

      if (columnExistsInStatement)
        continue;

      this->values.emplace_back(nullptr, header.tablePosition);
    }

    return true;
  }

  LogicalPlan* InsertStatement::ToLogical() {
    return new QueryPipeline::LogicalInsert(this->dbName, this->table, this->values);
  }

  bool CreateSchemaStatement::Validate(){
    if (Server::ServerInstance::Get().SchemaExists(this->dbName, this->name)) {
      cerr << "Schema " << this->name << " already exists"  << endl;
      return false;
    }

    return true;
  }

  QueryPipeline::LogicalPlan * CreateSchemaStatement::ToLogical(){
    return new LogicalSchemaCreate(this->dbName, this->name);
  }


}
