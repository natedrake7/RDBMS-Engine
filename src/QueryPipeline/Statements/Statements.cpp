#include "Statements.h"

#include "../Constants.h"
#include "../../AdditionalLibraries/Functions/StringFunctions.h"
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


    return true;
    // return this->where.expression->Validate(columnsDict);
  }

  LogicalPlan * DeleteStatement::ToLogical(){
    return new LogicalDelete(this->databaseId, this->table, this->where.expression);
  }

  JoinStatement::JoinStatement() {
    this->type = Constants::JoinType::Inner;
    this->table = nullptr;
    this->expression = nullptr;
  }

  JoinStatement::~JoinStatement(){
      delete this->table;
  }

  bool JoinStatement::Validate(){
    const auto header = Server::ServerInstance::Get().SelectTable(this->databaseId, this->table->name, this->table->schema);

    if (header.id == Constants::INVALID_TABLE_ID) {
      cerr << "Table " +this->table->schema + "." +this->table->name + " does not exist" << endl;
      return false;
    }

    this->table->tableId = header.id;
    this->table->ordinalPosition = header.ordinalPosition;

    return true;
  }

  QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){

      return nullptr;
    // return new LogicalJoin(
    //     this->databaseId,
    //     // new LogicalTableScan(),
    //     // this->joinType,
    //     // this->table2,
    //     // this->on.expression
    // );
  }

  CreateTableStatement::~CreateTableStatement() {
      delete this->constraint;
      delete this->table;

      for(const auto& column : this->columns)
          delete column;
  }

  // QueryPipeline::LogicalPlan * JoinStatement::ToLogical(){
  //   return new LogicalJoin(this->databaseId, this->table, this->joinType, this->table2, this->on.expression);
  // }

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

    const auto& schemasDict = Server::ServerInstance::Get().SelectSchemasToDictionary(this->databaseId);

    Headers::SchemaHeader schemaHeader;
    if (!schemasDict.TryGetValue(this->table->schema, schemaHeader)) {
      std::cerr << "Schema: " << this->table->schema << "does not exist." << std::endl;
      return false;
    }

    this->table->schemaId = schemaHeader.id;

    column_index_t tablePosition = 0;
    bool primaryKeyFound = false;
    Dictionary<string, column_index_t> columnNamesToIndexes;

    for (const auto& column: this->columns) {
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
      std::cerr << "Cannot have a primary key and a constraint declared" << std::endl;
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

   SelectStatement::~SelectStatement(){
      delete this->table;
      delete this->orderBy;
  }

  bool SelectStatement::Validate(){
    Dictionary<std::string, Constants::table_id_t> aliasesDictionary;

    if (this->table != nullptr
      && !this->table->Validate(this->databaseId))
      return false;

    if (!this->joins.empty() && this->table == nullptr) {
      std::cerr << "Table was not specified" << std::endl;
      return false;
    }

    //resolve expressions here since no column is to be used
    if (this->table == nullptr) {
      for (const auto& resultExpr : this->results){
        if (!ResolveExpressionAliases(this, resultExpr))
          return false;
      }

      return true;
    }

    for (const auto& join: this->joins) {
      if (!join->Validate())
        return false;
    }

    if (!ResolveAliases(aliasesDictionary, this))
      return false;

    return true;
  }

   WhereClause::WhereClause() { this->expression = nullptr; }

  bool WhereClause::IsValid() const{
    return this->expression != nullptr
      && (dynamic_cast<Expressions::BinaryExpression*>(this->expression) != nullptr
          || dynamic_cast<Expressions::LogicalExpression*>(this->expression) != nullptr);
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

   TableName::TableName(){ this->schema = "dbo"; }

  std::string TableName::GetFullName() const {
    return this->database + "." + this->schema + "." + this->name;
  }

  bool TableName::Validate(int32_t& selectedDatabaseId) {
    const auto tableHeader = (!this->database.empty())
        ? Server::ServerInstance::Get().SelectTable(this->database, this->name)
        : Server::ServerInstance::Get().SelectTable(selectedDatabaseId, this->name, this->schema);

    if (tableHeader.id == -1){
      std::cerr << "Table " + this->GetFullName() + " does not exist" << std::endl;
      return false;
    }

    this->tableId = tableHeader.id;
    this->ordinalPosition = tableHeader.ordinalPosition;
    this->databaseId = tableHeader.databaseId;

    return true;
  }

  LogicalPlan * SelectStatement::ToLogical(){
    LogicalPlan* current = (this->table != nullptr)
            ? new LogicalTableScan(this->databaseId, this->table, this->joins.empty() ? this->where.expression : nullptr)
            : nullptr;

    //join re orders take place here
    Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary;
    if (this->table != nullptr) {
      Constants::column_index_t columnIndex = 0;

      for (const auto &columnsDict : this->tableColumnsDictionary | views::values) {
        for (const auto &column: columnsDict | views::values) {
            if (columnIndicesDictionary.Contains(column.id))
              continue;

            columnIndicesDictionary.Add(column.id, columnIndex + column.ordinalPosition);
        }

        columnIndex += columnsDict.size();
      }

      AssignColumnsToIndices(this, columnIndicesDictionary);
    }


    //here create logical joins with the expressions
    for (const auto& join : this->joins) {
      //build logicalJoin
    }

    if (this->where.expression != nullptr) {
      //do the same for joins
      AssignColumnIndicesToResultExpression(this, columnIndicesDictionary, this->where.expression);
    }

    if (!this->results.empty())
      current = new LogicalProject(this->databaseId, current, this->results, this->columnHeaders);

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

  //TODO validate length of columns to match max record_size from master DB
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

      //check for default Values

      const auto defaultValue = Server::ServerInstance::Get().SelectDefaultValueByColumnId(header.id);

      if (!columnExistsInStatement
          && !header.isNullable
          && !identityColumns.Contains(header.id)
          && defaultValue.columnId == -1) {
        std::cerr << "Column " << columnName << " does not allow NULLS. Insert fails" << std::endl;
        return false;
      }

      if (columnExistsInStatement
        || identityColumns.Contains(header.id)
        || defaultValue.columnId != -1)
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

    for(auto&
      [name, value]: this->columns) {
      Headers::ColumnHeader header;

      if (!columnsDict.TryGetValue(name.name, header)) {
        cerr << "Column " << name.name << " does not exist on table: " << this->table->GetFullName() << endl;
        return false;
      }

      value.Validate(header);
    }

    if(this->where.expression == nullptr)
      return true;

    return true;
    // return this->where.expression->Validate(columnsDict);
  }

  QueryPipeline::LogicalPlan* UpdateStatement::ToLogical(){
    vector<Value> columnsUpdates;

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

    if (!this->addColumn->isNullable
      && this->addColumn->defaultValue.GetIsNull()) {
      std::cerr << "Cannot insert default Value NULL when NOT NULL is specified" << std::endl;
      return false;
    }

    this->addColumn->index = headers.size();

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(AdditionalLibraries::StringFunctions::NormalizeString(this->addColumn->type.name), columnType)) {
      std::cerr << "Invalid Column Type " << this->addColumn->type.name << std::endl;
      return false;
    }

    this->addColumn->defaultValue.Validate(columnType, this->addColumn->index);

    return true;
  }

  bool AlterTableStatement::ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const{
    Headers::ColumnHeader header;

    if (!headers.TryGetValue(this->alterColumn->name.name, header)) {
      std::cerr << "Column " << this->alterColumn->name.name << " does not exist on table: " << this->table->GetFullName() << std::endl;
      return false;
    }

    Constants::DataType columnType;
    if (!ColumnTypesDictionary.TryGetValue(AdditionalLibraries::StringFunctions::NormalizeString(this->alterColumn->type.name), columnType)) {
      std::cerr << "Invalid Column Type " << this->alterColumn->type.name << std::endl;
      return false;
    }

    if ((PipelineConstants::ValidStringConversions.Contains(columnType)
      && !PipelineConstants::ValidStringConversions.Contains(static_cast<Constants::DataType>(header.dataType)))
      || (PipelineConstants::ValidIntegerConversions.Contains(columnType)
        && !PipelineConstants::ValidIntegerConversions.Contains(static_cast<Constants::DataType>(header.dataType)))){
          std::cerr << "Cannot alter column " << this->alterColumn->name.name << " from type: "
                    << ColumnTypesToStringDictionary.Get(static_cast<Constants::DataType>(header.dataType))
                    << "to type: " << this->alterColumn->type.name << std::endl;

        return false;
    }

    if (header.recordSize > this->alterColumn->type.size) {
      std::cerr << "Cannot alter column " << this->alterColumn->type.name
                << " with size " <<  header.recordSize << " to size: " << this->alterColumn->type.size
                << std::endl
                << "Use FORCE if potential data corruption is acceptable" << std::endl;
      return false;
    }

    this->alterColumn->columnId = header.id;
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
    //Add Base Table to the dictionaries
    tableAliasesDictionary.Add(statement->table->alias.empty() ? statement->table->GetFullName() : statement->table->alias, statement->table->tableId);
    statement->tableColumnsDictionary.Add(statement->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(statement->table->tableId));

    //Add all the join tables to the dictionaries
    for (const auto& join: statement->joins) {
      tableAliasesDictionary.Add(join->table->alias.empty() ? join->table->GetFullName() : join->table->alias, join->table->tableId);
      statement->tableColumnsDictionary.Add(join->table->tableId, Server::ServerInstance::Get().SelectColumnsToDictionary(join->table->tableId));
    }

    //start resolving aliases
    for (int i = 0; i < statement->results.size(); i++)
      if (!ResolveExpressionAliases(tableAliasesDictionary, statement->tableColumnsDictionary, statement, statement->results[i], i))
          return false;

    //validate all expressions are valid
    if (statement->where.expression != nullptr) {
      if (!statement->where.IsValid()) {
        std::cerr << "Where expression must be either a logical or a binary expression" << std::endl;
        return false;
      }

      if (!ResolveExpressionAliases(tableAliasesDictionary,statement->tableColumnsDictionary, statement, statement->where.expression, 0))
        return false;
    }

    //validate join expressions
    for (const auto& join: statement->joins)
      if (!ResolveExpressionAliases(tableAliasesDictionary,statement->tableColumnsDictionary, statement, join->expression, 0))
        return false;

    if (statement->orderBy == nullptr)
      return true;

    for (auto& column: statement->orderBy->columns) {
      if (!ResolveColumnAlias(column, tableAliasesDictionary, statement->tableColumnsDictionary, statement))
        return false;
    }

    return true;
  }

  bool ResolveColumnAlias(
      Expressions::ColumnExpression* column,
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
      SelectStatement *statement,
      const int& indexPos){

        if (column->name == "*")
          return ResolveWildCardAlias(column, tableAliasesDictionary, tablesColumnsDictionary, statement, indexPos);

        if (!column->tableAlias.empty()) {
          table_id_t tableId;

          if (!tableAliasesDictionary.TryGetValue(column->tableAlias, tableId)) {
            cerr << "Alias: " << column->tableAlias << " does not exist in the statement" << endl;
            return false;
          }

          column->tableId = tableId;
        }

        bool columnExistsOnTable = false;
        Headers::ColumnHeader columnHeader;
        for (const auto& [key, columns]: tablesColumnsDictionary) {
          if (!columns.TryGetValue(column->name, columnHeader))
            continue;

          if (!columnExistsOnTable) {
            columnExistsOnTable = true;
            column->tableId = key;
            column->columnId = columnHeader.id;
            column->returnType = static_cast<Constants::DataType>(columnHeader.dataType);
            continue;
          }

          // ambigiousColumn = true;
          // break;
        }

        // if (ambigiousColumn) {
        //   cerr << "Ambigious Column: " << column.name << std::endl;
        //   return false;
        // }

      if (!columnExistsOnTable) {
        std::cerr << "Column: " << column->name << " does not exist on Table" << std::endl;
        return false;
      }

      if (column->alias.empty())
        column->alias = columnHeader.name;

      return true;
  }

  bool ResolveExpressionAliases(
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement,
    Expressions::Expression *expr,
    const int& indexPos){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr))
      return  ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->left, indexPos) &&
              ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, binaryExpr->right, indexPos);

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
      if (statement->table == nullptr) {
        std::cerr << "No table was specified but column with name: " << columnExpr->name << " was specified." << std::endl;
        return false;
      }

      return ResolveColumnAlias(columnExpr, tableAliasesDictionary, statement->tableColumnsDictionary, statement, indexPos);
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {

      //validate children expressions and assign return types and ids to column expressions
      for (auto* childExpr : functionExpr->arguments) {
        if (!ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, childExpr, indexPos))
          return false;
      }

      //validate number of arguments
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        std::cerr << errorMessage << std::endl;
        return false;
      }
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->left, indexPos)
        && ResolveExpressionAliases(tableAliasesDictionary, tablesColumnsDictionary, statement, logicalExpr->right, indexPos);
    }

    return true;
  }

  bool ResolveExpressionAliases(SelectStatement *statement, Expressions::Expression *expr){
    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr))
      return  ResolveExpressionAliases(statement, binaryExpr->left) &&
              ResolveExpressionAliases(statement, binaryExpr->right);

    if (const auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {
        std::cerr << "No table was specified but column with name: " << columnExpr->name << " was specified." << std::endl;
        return false;
    }

    if (const auto* functionExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      //validate functionExpression
      std::string errorMessage;
      if (!functionExpr->ValidateNumberOfArguments(errorMessage)) {
        std::cerr << errorMessage << std::endl;
        return false;
      }

      for (auto* childExpr : functionExpr->arguments) {
        if (!ResolveExpressionAliases(statement, childExpr))
          return false;
      }

    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      //validate type

      return ResolveExpressionAliases(statement, logicalExpr->left)
        && ResolveExpressionAliases(statement, logicalExpr->right);
    }

    //TODO Validate Literals and functions

    return true;
  }

  bool ResolveColumnAlias(
    ColumnName &column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary,
    SelectStatement *statement
    ){
      if (!column.alias.empty()) {
        table_id_t tableId;

        if (!tableAliasesDictionary.TryGetValue(column.alias, tableId)) {
          cerr << "Alias: " << column.alias << " does not exist in the statement" << endl;
          return false;
        }

        column.tableId = tableId;
      }

      bool columnExistsOnTable = false;
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

        // ambigiousColumn = true;
        // break;
      }

      // if (ambigiousColumn) {
      //   cerr << "Ambigious Column: " << column.name << std::endl;
      //   return false;
      // }

      if (!columnExistsOnTable) {
        std::cerr << "Column: " << column.name << " does not exist on Table" << std::endl;
        return false;
      }

      return true;
  }

  bool ResolveWildCardAlias(
    const Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t> &tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>> &tablesColumnsDictionary,
    SelectStatement *statement,
    const int& indexPos){

    if (column->name != "*")
      return true;

    table_id_t tableId = 0;
    if (!column->alias.empty()
      && !tableAliasesDictionary.TryGetValue(column->alias, tableId)) {
      std::cerr << "Alias " << column->alias << " does on exist on statement" << std::endl;
      return false;
    }

    if (column->alias.empty())
      tableId = statement->table->tableId;

    //remove the wildcard
    statement->results.erase(statement->results.begin() + indexPos);

    int counter = 0; //insert after IndexPos, the position of the
    for (const auto &header: tablesColumnsDictionary.Get(tableId) | views::values) {

      auto* columnExpression = new Expressions::ColumnExpression(
            header.name,
        statement->table->alias.empty()
        ? statement->table->GetFullName()
            : statement->table->alias
        );

      columnExpression->alias = header.name;
      columnExpression->columnId = header.id;
      columnExpression->tableId = statement->table->tableId;

      statement->results.insert(statement->results.begin() + indexPos + counter, columnExpression);
      counter++;
    }

    delete column;

    return true;
  }

  void AssignColumnsToIndices(SelectStatement *statement, Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary){
    for (const auto& resultExpr : statement->results)
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, resultExpr);

    if (statement->where.expression != nullptr) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, statement->where.expression);
    }

    if (statement->orderBy != nullptr) {
      for (auto& column : statement->orderBy->columns)
        statement->orderBy->columnIndices.emplace_back(columnIndicesDictionary.Get(column.columnId));
    }

    //group by here later
  }

  void AssignColumnIndicesToResultExpression(
    SelectStatement *statement,
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression *expr){

    if (const auto* binaryExpr = dynamic_cast<Expressions::BinaryExpression*>(expr)) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, binaryExpr->left);
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, binaryExpr->right);

      return;
    }

    if (auto* columnExpr = dynamic_cast<Expressions::ColumnExpression*>(expr)) {

      columnExpr->columnIndex = columnIndicesDictionary.Get(columnExpr->columnId);
      return;
    }

    if (const auto* funcExpr = dynamic_cast<Expressions::FunctionExpression*>(expr)) {
      for (auto* childExpr : funcExpr->arguments)
        AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, childExpr);

      return;
    }

    if (const auto* logicalExpr = dynamic_cast<Expressions::LogicalExpression*>(expr)) {
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, logicalExpr->left);
      AssignColumnIndicesToResultExpression(statement, columnIndicesDictionary, logicalExpr->right);

      return;
    }
  }

};