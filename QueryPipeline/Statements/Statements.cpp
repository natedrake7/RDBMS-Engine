#include "Statements.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

#include <iostream>
#include <ranges>

namespace QueryPipeline::Statements {
  bool DeleteStatement::Validate(){
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
          cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
          return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;

    if (this->where.expression == nullptr)
      return true;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(tableHeader.id);

    return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->databaseId, this->table, this->where.expression);
  }

  CreateTableStatement::~CreateTableStatement() {
      delete this->constraint;
      delete this->table;

      for(const auto& column : this->columns)
          delete column;
  }

  CreateTableStatement::CreateTableStatement(){
    this->table = nullptr;
    this->constraint = nullptr;
  }

  bool CreateTableStatement::Validate(){
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id != -1) {
      std::cerr << "Table with name: " << this->table->GetFullName() << " already exists." << std::endl;
      return false;
    }

    const auto schemas = Server::ServerInstance::Get().SelectSchemas(this->databaseId);

    int32_t schemaId = -1;
    for(const auto& schema: schemas){
      if(schema.name == this->table->schema){
        schemaId = schema.id;
        break;
      }
    }

    if(schemaId == -1){
      cerr << "Schema: " << this->table->schema << "does not exist." << endl;
      return false;
    }

    this->table->schemaId = schemaId;

    column_index_t tablePosition = 0;
    bool primaryKeyFound = false;
    Dictionary<string, column_index_t> columnNamesToIndexes;

    for (auto& column: this->columns) {
      uint16_t columnSize;

      if (!ColumnTypeSizes.TryGetValue(column->type.name, columnSize)) {
        std::cerr << "Column Type: " + column->type.name + " does not exist" << endl;
        return false;
      }

      if (columnSize != 0)
        column->type.size = columnSize;

      if (column->type.beforeFraction != 0 || column->type.afterFraction != 0) {
        //decimal handle
      }

      column->index = tablePosition++;

      columnNamesToIndexes.Add(column->name.name, column->index);

      if(column->isPrimaryKey && primaryKeyFound){
        cerr << "Cannot have multiple primary keys defined. Consider declaring a composite key" << endl;
        return false;
      }

      if (column->isPrimaryKey) {
        this->primaryKey.push_back(column->index);
        primaryKeyFound = true;

        //store the pointer if found, else let it be null
        if(column->autoIncrementKey && column->autoIncrementKey->incrementFactor <= 0){
            cerr << "increment factor cannot be less or equal to 0" << endl;
            return false;
        }
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
      this->primaryKey.push_back(columnNamesToIndexes[column.name]);

    return true;
  }

  LogicalPlan * CreateTableStatement::ToLogical(){
    const auto constraintName = this->constraint == nullptr ? "" : this->constraint->name;

    return new LogicalTableCreate(this->databaseId, this->table, this->columns, this->primaryKey, constraintName);
  }

  bool SelectStatement::Validate(){
    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
          cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
          return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;

    for (auto& join: this->joins) {
      const auto joinHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, join->table->name, join->table->schema);

      if (joinHeader.id == -1) {
        cerr << "Table " + join->table->schema + "." + join->table->name + " does not exist" << endl;
        return false;
      }

      join->table->tableId = joinHeader.id;
      join->table->ordinalPosition = joinHeader.ordinalPosition;
    }

    if (!ResolveAliases(aliasesDictionary, this))
      return false;

    // if (this->where.expression != nullptr && !this->where.expression->Validate())

    return true;
    //
    // if (!this->columns.empty() && this->columns[0].name == "*") {
    //   this->columns.clear();
    //
    //   for (const auto &header : columnsDict | views::values){
    //     this->columnHeaders.emplace_back(header);
    //     this->columns.emplace_back(header.name);
    //     this->columnIndices.emplace_back(header.ordinalPosition);
    //    }
    // }
    // else {
    //   for (const auto& selectColumn : this->columns) {
    //     if (Headers::ColumnHeader header ;columnsDict.TryGetValue(selectColumn.name, header)) {
    //       this->columnHeaders.emplace_back(header);
    //       this->columnIndices.emplace_back(header.ordinalPosition);
    //       continue;
    //     }
    //
    //     cerr << "Column " + selectColumn.name + " does not exist" << endl;
    //   }
    // }

    // if (this->where.expression != nullptr && !this->where.expression->Validate(columnsDict))
    //   return false;
    //
    // if(!this->orderBy)
    //   return true;
    //
    // return this->orderBy->Validate(this->columns, columnsDict);

  }

  bool OrderByStatement::Validate(const std::vector<ColumnName>& selectColumns, const Dictionary<std::string, Headers::ColumnHeader>& columnsDict){

    HashSet<std::string> selectColumnMap;
    for (const auto& selectColumn : selectColumns) {
      selectColumnMap.Add(selectColumn.name);
    }

    for(const auto& column : this->columns){
      if(!selectColumnMap.Contains(column.name)){
        cerr << "Column " << column.name << " does not exist on the statement." << endl;
        return false;
      }

      Headers::ColumnHeader header;
      columnsDict.TryGetValue(column.name, header);

      this->columnIndices.emplace_back(header.ordinalPosition);
    }

    return true;
  }

  LogicalPlan * SelectStatement::ToLogical(){
    const auto scanTable = new LogicalTableScan(this->databaseId, this->table, this->joins.empty() ? this->where.expression : nullptr);

    LogicalPlan* current = scanTable;

    //join re orders take place here

    Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary;
    Constants::column_index_t columnIndex = 0;

    for (const auto& [key, columns] : this->tableColumnsDictionary) {
      for (const auto& [alias, column]: columns) {
          if (columnIndicesDictionary.Contains(column.id))
            continue;

          columnIndicesDictionary.Add(column.id, columnIndex + column.ordinalPosition);
      }

      columnIndex += columns.size();
    }

    AssignColumnsToIndices(this, columnIndicesDictionary);

    if (this->where.expression != nullptr) {
      //do the same for joins
      MapExpressionColumnsToIndices(this->where.expression, columnIndicesDictionary);
      current = new LogicalFilter(this->databaseId, current, this->where.expression);
    }

    if (!this->columns.empty())
      current = new LogicalProject(this->databaseId, current, this->columnIndices, this->columnHeaders);

    if(this->orderBy != nullptr){
      const auto orderType = this->orderBy->order == "DESC" ? OrderType::DESCENDING : OrderType::ASCENDING;

      current = new LogicalOrder(this->databaseId, current, this->orderBy->columnIndices, orderType);
    }

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

    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
          cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
          return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;
    
    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(tableHeader.id);

    const auto identityColumns = Server::ServerInstance::Get().SelectIdentityColumnsByTableIdToDictionary(tableHeader.id);

//    if (this->columns.size() <= this->values.size() + identityColumns.size()) {
//      cerr << "Invalid number of arguments supplied" << endl;
//      return false;
//    }

    for (int i = 0;i < this->columns.size(); i++) {
      const auto& column = this->columns[i];

      Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(column.name, header)) {
        cerr << "Column " << column.name << " does not exist on table: " << this->table->name << endl;
        return false;
      }

      this->values.at(i).Validate(header);
    }

    for (const auto&[columnName, header]:  columnsDict) {
      if (header.isSystem)
        continue;
      
      bool columnExistsInStatement = false;
      
      for (const auto& statementColumn: this->columns) {
        if (columnName != statementColumn.name)
          continue;
          
        columnExistsInStatement = true;
        break;
      }

      if (!columnExistsInStatement
          && !header.isNullable
          && !identityColumns.Contains(header.id)) {
        cerr << "Column " << columnName << " does not allow NULLS. Insert fails";
        return false;
      }

      if (columnExistsInStatement || identityColumns.Contains(header.id))
        continue;

      this->values.emplace_back(nullptr, header.ordinalPosition);
    }

    return true;
  }

  LogicalPlan* InsertStatement::ToLogical() {
    return new QueryPipeline::LogicalInsert(this->databaseId, this->table, this->values);
  }

  bool CreateSchemaStatement::Validate(){
    if (Server::ServerInstance::Get().SchemaExists(this->databaseId, this->name)) {
      cerr << "Schema " << this->name << " already exists"  << endl;
      return false;
    }

    return true;
  }

  QueryPipeline::LogicalPlan * CreateSchemaStatement::ToLogical(){
    return new LogicalSchemaCreate(this->databaseId, this->name);
  }


  bool UpdateStatement::Validate(){
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
          cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
          return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(this->table->tableId);

    for(auto& column: this->columns) {
      Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(column.name.name, header)) {
        cerr << "Column " << column.name.name << " does not exist on table: " << this->table->GetFullName() << endl;
        return false;
      }

      column.value.Validate(header);
    }

    if(this->where.expression == nullptr)
      return true;

    return this->where.expression->Validate(columnsDict);
  }

  QueryPipeline::LogicalPlan* UpdateStatement::ToLogical(){
    vector<Field> columnsUpdates;

    for(const auto& column: this->columns)
      columnsUpdates.emplace_back(column.value);

    return new QueryPipeline::LogicalUpdate(this->databaseId, this->table, columnsUpdates, this->where.expression);
  }

  bool CreateIndexStatement::Validate(){
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
      cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
      return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(tableHeader.id);

    for(auto& column: this->columns) {
      Headers::ColumnHeader header;

      if (columnsDict.TryGetValue(column, header)) {
        this->columnIndices.push_back(header.ordinalPosition);

        continue;
      }

      cerr << "Column " << column << " does not exist on table: " << this->table->schema << "." << this->table->name << endl;
      return false;
    }

    const auto indexes = Server::ServerInstance::Get().SelectIndexes(tableHeader.id);

    for (const auto& index: indexes) {
      const auto indexedColumns = Server::ServerInstance::Get().SelectIndexColumnsByIndexIdToDictionary(index.id);

      if (index.name == this->name) {
        cerr << "Index with name: " << index.name << " already exists" << endl;
        return false;
      }

      //check if identical index exists (no need for a duplicate).
    }

    return true;
  }

  QueryPipeline::LogicalPlan * CreateIndexStatement::ToLogical(){
    return new QueryPipeline::LogicalIndexCreate(this->databaseId, this->table, this->name, this->columnIndices);
  }

  bool AlterTableStatement::ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    if (headers.Contains(this->addColumn->name.name)) {
      std::cerr << "Column " << this->addColumn->name.name << " already exists on table: "<< this->table->GetFullName() << std::endl;
      return false;
    }

    this->addColumn->index = headers.size();
    return true;
  }

  bool AlterTableStatement::ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers){
    return true;
  }

  bool AlterTableStatement::ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    if (!headers.TryGetValue(this->dropColumn->name.name, header)) {
      std::cerr << "Column " << this->dropColumn->name.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    //validate no index or constraint uses it
    const auto constraints = Server::ServerInstance::Get().SelectConstraints(this->table->tableId);

    for (const auto& constraint: constraints) {
      const auto columns = Server::ServerInstance::Get().SelectConstraintColumnsByConstraintIdToDictionary(constraint.constraintId);

      if (columns.Contains(header.id)) {
        std::cerr << "Cannot drop column: " << header.name << " as it is referenced by constraint: " << constraint.name << std::endl;
        return false;
      }
    }

    this->dropColumn->index = header.ordinalPosition;

    return true;
  }

  bool AlterTableStatement::ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;
    if (!headers.TryGetValue(this->renameColumn->oldName.name, header)) {
      std::cerr << "Column " << this->renameColumn->oldName.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    this->renameColumn->columnId = header.id;
    this->renameColumn->ordinalPosition = header.ordinalPosition;

    return true;
  }

  bool AlterTableStatement::Validate(){
    const auto tableHeader = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (tableHeader.id == -1){
      cerr << "Table " + this->table->GetFullName() + " does not exist" << endl;
      return false;
    }

    this->table->tableId = tableHeader.id;
    this->table->ordinalPosition = tableHeader.ordinalPosition;

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary(tableHeader.id);

    //validate by type
    switch (this->type) {
      case AlterTableType::AddColumn:
        return this->ValidateAddColumn(columnsDict);
      case AlterTableType::AlterColumn:
        return this->ValidateAlterColumn(columnsDict);
      case AlterTableType::DropColumn:
        return this->ValidateDropColumn(columnsDict);
      case AlterTableType::RenameColumn:
        return this->ValidateRenameColumn(columnsDict);
      default:
        std::cerr << "Unknown table type" << std::endl;
        return false;
    }
  }

  QueryPipeline::LogicalPlan * AlterTableStatement::ToLogical(){
    return new LogicalAlterTable(this->databaseId, this->table, this->type, this->alterColumn, this->addColumn, this->dropColumn, this->renameColumn);
  }

  bool ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary, SelectStatement *statement){
    tableAliasesDictionary.Add(statement->table->alias.empty() ? statement->table->GetFullName() : statement->table->alias, statement->table->tableId);

    Dictionary<int32_t, Constants::column_index_t> computedColumnIndexes;

    statement->tableColumnsDictionary.Add(statement->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(statement->table->tableId));

    for (const auto& join: statement->joins) {
      tableAliasesDictionary.Add(join->table->alias.empty() ? join->table->GetFullName() : join->table->alias, join->table->tableId);
      statement->tableColumnsDictionary.Add(join->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(join->table->tableId));
    }

    for (int i = 0;i < statement->columns.size(); i++) {
      auto& column = statement->columns[i];

      if (column.name == "*") {
        if (!ResolveWildCardAlias(column, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
          return false;

        //no reason to check the column as they are valid and their aliases are set
        statement->columns.erase(statement->columns.begin() + i);
        continue;
      }

      if (!ResolveColumnAlias(column, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
        return false;
    }

    //validate all expressions are valid
    if (statement->where.expression != nullptr
      && !ResolveExpressionAliases(statement->where.expression, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
      return false;

    //validate join expressions
    for (const auto& join: statement->joins) {
      if (!ResolveExpressionAliases(join->expression, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
        return false;
    }

    if (statement->orderBy == nullptr)
      return true;

    for (auto& column: statement->orderBy->columns) {
      if (!ResolveColumnAlias(column, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
        return false;
    }

    return true;
  }

  bool ResolveColumnAlias(
      ColumnName& column,
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
      SelectStatement *statement){

        if (column.name == "*"
          && !ResolveWildCardAlias(column, tableAliasesDictionary, tablesColumnsDictionary, statement))
          return false;

        if (!column.alias.empty()) {
          table_id_t tableId;

          if (!tableAliasesDictionary.TryGetValue(column.alias, tableId)) {
            cerr << "Alias: " << column.alias << " does not exist in the statement" << endl;
            return false;
          }

          column.tableId = tableId;
        }

        bool columnExistsOnTable = false;
        bool ambigiousColumn = false;
        for (const auto& [key, columns]: tablesColumnsDictionary) {
          Headers::ColumnHeader columnHeader;
          if (!columns.TryGetValue(column.name, columnHeader))
            continue;

          if (!columnExistsOnTable) {
            columnExistsOnTable = true;
            column.tableId = key;
            column.columnId = columnHeader.id;
            continue;
          }

          ambigiousColumn = true;
          break;
        }

        if (ambigiousColumn) {
          cerr << "Ambigious Column: " << column.name << std::endl;
          return false;
        }

      if (!columnExistsOnTable) {
        std::cerr << "Column: " << column.name << " does not exist on Table" << std::endl;
        return false;
      }

      return true;
  }

  bool ResolveWildCardAlias(
    const ColumnName &column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary,
    SelectStatement *statement){

    if (column.name != "*")
      return true;

    if (column.alias.empty()) {
      auto& columnHeaders = tablesColumnsDictionary.Get(statement->table->tableId);
      for (const auto& [key, header]: columnHeaders) {

        ColumnName columnName{
          .name = header.name,
          .alias = statement->table->alias.empty() ? statement->table->GetFullName() : statement->table->alias,
          .tableId = statement->table->tableId,
          .columnId = header.id
        };

        statement->columns.push_back(std::move(columnName));
      }

      return true;
    }

    table_id_t tableId;
    if (!tableAliasesDictionary.TryGetValue(column.alias, tableId)) {
      std::cerr << "Alias " << column.alias << " does on exist on statement" << std::endl;
      return false;
    }

    return true;
  }

  bool ResolveExpressionAliases(
    Expressions::Expression *expression,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary,
    SelectStatement *statement){

    if (expression->type == Expressions::ExpressionType::Predicate)
      return ResolveColumnAlias(expression->column, tableAliasesDictionary, tablesColumnsDictionary, statement);

    if (expression->left == nullptr
      || expression->right == nullptr) {
      std::cerr << "Invalid expression specified" << std::endl;
      return false;
    }

    return ResolveExpressionAliases(expression->left, tableAliasesDictionary, tablesColumnsDictionary, statement)
        && ResolveExpressionAliases(expression->right, tableAliasesDictionary, tablesColumnsDictionary, statement);
  }

  void MapExpressionColumnsToIndices(Expressions::Expression *expression, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary){
      if (expression->type == Expressions::ExpressionType::Predicate) {
          expression->columnIndex = columnIndicesDictionary.Get(expression->column.columnId);
          return;
      }

      MapExpressionColumnsToIndices(expression->left, columnIndicesDictionary);
      MapExpressionColumnsToIndices(expression->right, columnIndicesDictionary);
  }

  void AssignColumnsToIndices(SelectStatement *statement, Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary){
    for (auto& column : statement->columns)
      statement->columnIndices.emplace_back(columnIndicesDictionary.Get(column.columnId));

    if (statement->orderBy != nullptr) {
      for (auto& column : statement->orderBy->columns)
        statement->orderBy->columnIndices.emplace_back(columnIndicesDictionary.Get(column.columnId));
    }

    //group by here later
  }

}