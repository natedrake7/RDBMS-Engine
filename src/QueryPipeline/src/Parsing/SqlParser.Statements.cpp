#include "../../include/Parsing/SqlParser.h"
#include "../../include/Statements.h"

#include "../../../CoreEngine/include/Evaluators/Expression.h"
#include "../../../Systemic/include/Converter.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace QueryPipeline::Parsing{

    /**
     * Lower cased type names, matching the keys of COLUMN_TYPENAMES_TO_ENUMS.
     * Statements::ColumnType stores the type by name rather than by enum, so the two
     * have to agree.
     */
    static DataTypes::StringView DataTypeName(const DataType type){
        switch (type){
            case DataType::String:   return DataTypes::StringView("string");
            case DataType::Bool:     return DataTypes::StringView("bool");
            case DataType::TinyInt:  return DataTypes::StringView("tinyint");
            case DataType::SmallInt: return DataTypes::StringView("smallint");
            case DataType::Int:      return DataTypes::StringView("int");
            case DataType::BigInt:   return DataTypes::StringView("bigint");
            case DataType::Decimal:  return DataTypes::StringView("decimal");
            case DataType::DateTime: return DataTypes::StringView("datetime");
            case DataType::Guid:     return DataTypes::StringView("guid");
            case DataType::Json:     return DataTypes::StringView("json");
            default:                 return DataTypes::StringView();
        }
    }

    static bool IsDataTypeToken(const TokenType type){
        switch (type){
            case TokenType::Bool:
            case TokenType::TinyInt:
            case TokenType::SmallInt:
            case TokenType::Int:
            case TokenType::BigInt:
            case TokenType::Decimal:
            case TokenType::DateTime:
            case TokenType::Guid:
            case TokenType::Json:
            case TokenType::String:
                return true;
            default:
                return false;
        }
    }

    bool SqlParser::IsClauseKeyword(const TokenType type){
        switch (type){
            case TokenType::From:
            case TokenType::Where:
            case TokenType::Order:
            case TokenType::By:
            case TokenType::Join:
            case TokenType::Inner:
            case TokenType::Left:
            case TokenType::Right:
            case TokenType::Full:
            case TokenType::Outer:
            case TokenType::On:
            case TokenType::Set:
            case TokenType::Values:
            case TokenType::Into:
            case TokenType::To:
                return true;
            default:
                return false;
        }
    }

    /**
     * @name Entry points
     * @{
     */

    bool SqlParser::ParseStatements(
        DataStructures::PolymorphicArray<Statements::Statement*>* statements,
        const DataTypes::Guid* sessionId,
        const Int databaseId
    ){
        if (this->AtEnd()){
            this->Fail("expected a statement");
            return false;
        }

        while (!this->AtEnd()){
            auto* statement = this->ParseStatement(sessionId, databaseId);
            if (statement == nullptr)
                return false;

            statements->Push(statement);

            if (!this->Expect(TokenType::Semicolon))
                return false;
        }

        return true;
    }

    Statements::Statement* SqlParser::ParseStatement(const DataTypes::Guid* sessionId, const Int databaseId){
        Statements::Statement* statement = nullptr;
        switch (this->Current().type){
            case TokenType::Select:
                statement = this->ParseSelect();
                break;
            case TokenType::Insert:
                statement = this->ParseInsert();
                break;
            case TokenType::Update:
                statement = this->ParseUpdate();
                break;
            case TokenType::Delete:
                statement = this->ParseDelete();
                break;
            case TokenType::Create:
                statement = this->ParseCreate();
                break;
            case TokenType::Drop:
                statement = this->ParseDropDatabase();
                break;
            case TokenType::Use:
                statement = this->ParseUseDatabase();
                break;
            case TokenType::Grant:
                statement = this->ParseGrantRole();
                break;
            case TokenType::Alter:
                statement = this->ParseAlterTable();
                break;
            case TokenType::Declare:
                statement = this->ParseVariableDeclaration(true);
                break;
            case TokenType::Set:
                statement = this->ParseVariableDeclaration(false);
                break;
            default:
                statement = this->Fail("expected the start of a statement");
                break;
        }

        if (statement != nullptr){
            statement->sessionId = *sessionId;
            statement->databaseId = databaseId;
        }

        return statement;
    }

    /** @} */

    Statements::Statement* SqlParser::ParseSelect(){
        this->Advance(); //SELECT

        auto* statement = this->_allocator->Allocate<Statements::SelectStatement>(this->_allocator);

        if (this->Check(TokenType::Top) && !this->ParseTop(statement->top))
            return nullptr;

        statement->distinct = this->Match(TokenType::Distinct);

        do {
            Expressions::Expression* projection = nullptr;
            if (!this->ParseSelectItem(projection))
                return nullptr;

            statement->_projections.Push(projection);
        }
        while (this->Match(TokenType::Comma));

        if (this->Match(TokenType::From)){
            statement->table = this->ParseDataSource();
            if (statement->table == nullptr)
                return nullptr;
        }

        while (this->Check(TokenType::Join)  || this->Check(TokenType::Inner)
            || this->Check(TokenType::Left)  || this->Check(TokenType::Right)
            || this->Check(TokenType::Full)){
            auto* join = this->ParseJoin();
            if (join == nullptr)
                return nullptr;

            statement->_joins.Push(join);
        }

        if (this->Check(TokenType::Where) && !this->ParseWhereClause(statement->where))
            return nullptr;

        if (!this->Check(TokenType::Order))
            return statement;

        statement->orderBy = this->ParseOrderBy();

        return (statement->orderBy == nullptr) ? nullptr : statement;
    }

    bool SqlParser::ParseTop(BigInt& top){
        this->Advance(); //TOP

        const auto parenthesised = this->Match(TokenType::LeftParen);

        if (!this->Check(TokenType::IntegerLiteral)){
            this->Fail("expected a row count after TOP");
            return false;
        }

        const auto& count = this->Advance();
        if (!Converter::TryStrToInt<BigInt>(count.View())){
            this->Fail("the TOP row count is out of range");
            return false;
        }

        top = Converter::StrToInt<BigInt>(count.View());

        return !parenthesised || this->Expect(TokenType::RightParen);
    }

    bool SqlParser::ParseSelectItem(Expressions::Expression*& expression){
        expression = this->ParseExpression();
        if (expression == nullptr)
            return false;

        //Expression::name carries the output alias, empty when none was written.
        auto alias = DataTypes::String::Empty(this->_allocator);
        if (!this->ParseOptionalAlias(alias))
            return false;

        expression->name = std::move(alias);
        return true;
    }

    Statements::DataSource* SqlParser::ParseDataSource(){
        if (!this->Check(TokenType::LeftParen))
            return this->ParseTableName(true);

        //The grammar allows a subquery here, but SelectStatement::table is a DataSource*
        //with nowhere to hang one, so this has never actually worked.
        return this->Fail("subqueries in FROM are not supported yet");
    }

    Statements::DataSource* SqlParser::ParseTableName(const bool allowAlias){
        DataTypes::String parts[3];
        Int count = 0;

        do {
            if (count == 3)
                return this->Fail("a table name may have at most three parts");

            if (!this->ParseIdentifier(parts[count]))
                return nullptr;

            ++count;
        }
        while (this->Match(TokenType::Dot));

        auto* source = this->_allocator->Allocate<Statements::DataSource>(this->_allocator);

        //database.schema.name, schema.name, or a bare name.
        if (count == 3){
            source->database = std::move(parts[0]);
            source->schema = std::move(parts[1]);
        }
        else if (count == 2)
            source->schema = std::move(parts[0]);

        source->name = std::move(parts[count - 1]);

        if (!allowAlias)
            return source;

        return this->ParseOptionalAlias(source->alias) ? source : nullptr;
    }

    Statements::JoinStatement* SqlParser::ParseJoin(){
        auto* statement = this->_allocator->Allocate<Statements::JoinStatement>();

        switch (this->Current().type){
            case TokenType::Inner:
                this->Advance();
                statement->type = JoinType::Inner;
                break;

            case TokenType::Left:
                this->Advance();
                this->Match(TokenType::Outer);
                statement->type = JoinType::Left;
                break;

            case TokenType::Right:
                this->Advance();
                this->Match(TokenType::Outer);
                statement->type = JoinType::Right;
                break;

            case TokenType::Full:
                this->Advance();
                this->Match(TokenType::Outer);
                statement->type = JoinType::Full;
                break;

            default:
                statement->type = JoinType::Inner;
                break;
        }

        if (!this->Expect(TokenType::Join))
            return nullptr;

        statement->table = this->ParseTableName(true);
        if (statement->table == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::On))
            return nullptr;

        statement->expression = this->ParseExpression();

        return (statement->expression == nullptr) ? nullptr : statement;
    }

    bool SqlParser::ParseWhereClause(Statements::WhereClause& where){
        this->Advance(); //WHERE

        where.expression = this->ParseExpression();

        return where.expression != nullptr;
    }

    Statements::OrderByStatement* SqlParser::ParseOrderBy(){
        this->Advance(); //ORDER

        if (!this->Expect(TokenType::By))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::OrderByStatement>();
        statement->columns.TrySetAllocator(this->_allocator);

        do {
            auto* column = this->_allocator->Allocate<Statements::OrderColumn>();

            column->expression = this->ParseExpression();
            if (column->expression == nullptr)
                return nullptr;

            if (this->Match(TokenType::Desc))
                column->type = Constants::OrderType::DESCENDING;
            else {
                this->Match(TokenType::Asc);
                column->type = Constants::OrderType::ASCENDING;
            }

            statement->columns.Push(column);
        }
        while (this->Match(TokenType::Comma));

        return statement;
    }

    Statements::Statement* SqlParser::ParseInsert(){
        this->Advance(); //INSERT

        if (!this->Expect(TokenType::Into))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::InsertStatement>();

        statement->columns.TrySetAllocator(this->_allocator);
        statement->values.TrySetAllocator(this->_allocator);
        statement->selectStatement = nullptr;

        statement->table = this->ParseTableName(false);
        if (statement->table == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        do {
            Statements::ColumnName column;
            if (!this->ParseColumnName(column))
                return nullptr;

            statement->columns.Push(column);
        }
        while (this->Match(TokenType::Comma));

        if (!this->Expect(TokenType::RightParen))
            return nullptr;

        if (this->Check(TokenType::Select)){
            statement->selectStatement = static_cast<Statements::SelectStatement*>(this->ParseSelect());
            return (statement->selectStatement == nullptr) ? nullptr : statement;
        }

        if (!this->Expect(TokenType::Values))
            return nullptr;

        do {
            if (!this->Expect(TokenType::LeftParen))
                return nullptr;

            Statements::Inserts row;
            row.values.TrySetAllocator(this->_allocator);

            do {
                auto* value = this->ParseExpression();
                if (value == nullptr)
                    return nullptr;

                row.values.Push(value);
            }
            while (this->Match(TokenType::Comma));

            if (!this->Expect(TokenType::RightParen))
                return nullptr;

            statement->values.Push(std::move(row));
        }
        while (this->Match(TokenType::Comma));

        return statement;
    }

    Statements::Statement* SqlParser::ParseUpdate(){
        this->Advance(); //UPDATE

        auto* statement = this->_allocator->Allocate<Statements::UpdateStatement>();
        statement->updates.TrySetAllocator(this->_allocator);

        statement->table = this->ParseTableName(true);
        if (statement->table == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::Set))
            return nullptr;

        do {
            auto* update = this->_allocator->Allocate<Statements::UpdateColumn>();

            if (!this->ParseColumnName(update->name))
                return nullptr;

            if (!this->Expect(TokenType::Equal))
                return nullptr;

            update->value = this->ParseExpression();
            if (update->value == nullptr)
                return nullptr;

            statement->updates.Push(update);
        }
        while (this->Match(TokenType::Comma));

        if (this->Check(TokenType::Where) && !this->ParseWhereClause(statement->where))
            return nullptr;

        return statement;
    }

    Statements::Statement* SqlParser::ParseDelete(){
        this->Advance(); //DELETE

        if (!this->Expect(TokenType::From))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::DeleteStatement>();

        statement->table = this->ParseTableName(true);
        if (statement->table == nullptr)
            return nullptr;

        if (this->Check(TokenType::Where) && !this->ParseWhereClause(statement->where))
            return nullptr;

        return statement;
    }

    Statements::Statement* SqlParser::ParseCreate(){
        this->Advance(); //CREATE

        switch (this->Current().type){
            case TokenType::Table:    return this->ParseCreateTable();
            case TokenType::Schema:   return this->ParseCreateSchema();
            case TokenType::Database: return this->ParseCreateDatabase();
            case TokenType::User:     return this->ParseCreateUser();
            case TokenType::Index:    return this->ParseCreateIndex(false);

            case TokenType::Unique:
                this->Advance();
                return this->ParseCreateIndex(true);

            default:
                return this->Fail("expected TABLE, INDEX, SCHEMA, DATABASE or USER after CREATE");
        }
    }

    Statements::Statement* SqlParser::ParseCreateTable(){
        this->Advance(); //TABLE

        auto* statement = this->_allocator->Allocate<Statements::CreateTableStatement>();

        statement->columns.TrySetAllocator(this->_allocator);
        statement->primaryKey.TrySetAllocator(this->_allocator);

        statement->table = this->ParseTableName(false);
        if (statement->table == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        do {
            //A trailing PRIMARY KEY (...) is a table constraint, not another column,
            //and the grammar only allows it last.
            if (this->Check(TokenType::Constraint) || this->Check(TokenType::Primary)){
                statement->constraint = this->ParsePrimaryKeyConstraint();
                if (statement->constraint == nullptr)
                    return nullptr;

                break;
            }

            auto* column = this->ParseNewColumn();
            if (column == nullptr)
                return nullptr;

            statement->columns.Push(column);
        }
        while (this->Match(TokenType::Comma));

        return this->Expect(TokenType::RightParen) ? statement : nullptr;
    }

    Statements::NewColumn* SqlParser::ParseNewColumn(){
        auto* column = this->_allocator->Allocate<Statements::NewColumn>();

        column->identity = nullptr;
        column->isPrimaryKey = false;
        column->defaultValue = Value::Null();

        if (!this->ParseColumnName(column->name))
            return nullptr;

        if (!this->ParseColumnType(column->type))
            return nullptr;

        if (this->Match(TokenType::Primary)){
            if (!this->Expect(TokenType::Key))
                return nullptr;

            column->isPrimaryKey = true;

            if (this->Check(TokenType::Identity)){
                column->identity = this->ParseIdentity();
                if (column->identity == nullptr)
                    return nullptr;
            }
        }

        bool hasNotNull = false;
        bool hasNull = false;

        if (this->Match(TokenType::Not)){
            if (!this->Expect(TokenType::Null))
                return nullptr;

            hasNotNull = true;
        }
        else
            hasNull = this->Match(TokenType::Null);

        if (column->isPrimaryKey && hasNull)
            return this->Fail("a primary key column cannot be declared NULL");

        column->isNullable = !column->isPrimaryKey && !hasNotNull;

        if (!this->Match(TokenType::Default))
            return column;

        if (column->isPrimaryKey)
            return this->Fail("a primary key column cannot have a default value");

        return this->ParseLiteralValue(column->defaultValue) ? column : nullptr;
    }

    Statements::Identity* SqlParser::ParseIdentity(){
        this->Advance(); //IDENTITY

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        auto* identity = this->_allocator->Allocate<Statements::Identity>();

        if (!this->Check(TokenType::IntegerLiteral) || !Converter::TryStrToInt<SmallInt>(this->Current().View()))
            return this->Fail("expected an identity seed");

        identity->seed = Converter::StrToInt<SmallInt>(this->Advance().View());

        if (!this->Expect(TokenType::Comma))
            return nullptr;

        if (!this->Check(TokenType::IntegerLiteral) || !Converter::TryStrToInt<SmallInt>(this->Current().View()))
            return this->Fail("expected an identity increment");

        identity->incrementFactor = Converter::StrToInt<SmallInt>(this->Advance().View());
        identity->cacheBlock = Constants::DEFAULT_IDENTITY_CACHE_BLOCK;

        return this->Expect(TokenType::RightParen) ? identity : nullptr;
    }

    Statements::PrimaryKeyConstraint* SqlParser::ParsePrimaryKeyConstraint(){
        auto* constraint = this->_allocator->Allocate<Statements::PrimaryKeyConstraint>();
        constraint->columns.TrySetAllocator(this->_allocator);

        if (this->Match(TokenType::Constraint) && !this->ParseIdentifier(constraint->name))
            return nullptr;

        if (!this->Expect(TokenType::Primary) || !this->Expect(TokenType::Key))
            return nullptr;

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        do {
            Statements::ColumnName column;
            if (!this->ParseColumnName(column))
                return nullptr;

            constraint->columns.Push(column);
        }
        while (this->Match(TokenType::Comma));

        return this->Expect(TokenType::RightParen) ? constraint : nullptr;
    }

    Statements::Statement* SqlParser::ParseCreateIndex(const bool isUnique){
        if (!this->Expect(TokenType::Index))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::CreateIndexStatement>();

        statement->columns.TrySetAllocator(this->_allocator);
        statement->columnIndices.TrySetAllocator(this->_allocator);
        statement->isUnique = isUnique;

        if (!this->ParseIdentifier(statement->name))
            return nullptr;

        if (!this->Expect(TokenType::On))
            return nullptr;

        statement->table = this->ParseTableName(false);
        if (statement->table == nullptr)
            return nullptr;

        if (!this->Expect(TokenType::LeftParen))
            return nullptr;

        do {
            Statements::ColumnName column;
            if (!this->ParseColumnName(column))
                return nullptr;

            statement->columns.Push(std::move(column.name));
        }
        while (this->Match(TokenType::Comma));

        return this->Expect(TokenType::RightParen) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseCreateSchema(){
        this->Advance(); //SCHEMA

        auto* statement = this->_allocator->Allocate<Statements::CreateSchemaStatement>();

        return this->ParseIdentifier(statement->name) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseCreateDatabase(){
        this->Advance(); //DATABASE

        auto* statement = this->_allocator->Allocate<Statements::CreateDbStatement>();

        return this->ParseIdentifier(statement->name) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseCreateUser(){
        this->Advance(); //USER

        auto* statement = this->_allocator->Allocate<Statements::CreateUserStatement>();

        if (!this->ParseIdentifier(statement->username))
            return nullptr;

        if (!this->Expect(TokenType::With) || !this->Expect(TokenType::Password))
            return nullptr;

        if (!this->Check(TokenType::StringLiteral))
            return this->Fail("expected a quoted password");

        statement->password = this->MakeString(this->Advance());

        if (!this->Expect(TokenType::And) || !this->Expect(TokenType::Role))
            return nullptr;

        return this->ParseIdentifier(statement->role) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseDropDatabase(){
        this->Advance(); //DROP

        if (!this->Expect(TokenType::Database))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::DropDbStatement>();

        return this->ParseIdentifier(statement->name) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseUseDatabase(){
        this->Advance(); //USE

        if (!this->Expect(TokenType::Database))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::UseDatabaseStatement>();

        return this->ParseIdentifier(statement->name) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseGrantRole(){
        this->Advance(); //GRANT

        auto* statement = this->_allocator->Allocate<Statements::GrantRoleStatement>();

        if (!this->ParseIdentifier(statement->role))
            return nullptr;

        if (!this->Expect(TokenType::To))
            return nullptr;

        return this->ParseIdentifier(statement->username) ? statement : nullptr;
    }

    Statements::Statement* SqlParser::ParseAlterTable(){
        this->Advance(); //ALTER

        if (!this->Expect(TokenType::Table))
            return nullptr;

        auto* statement = this->_allocator->Allocate<Statements::AlterTableStatement>();

        statement->table = this->ParseTableName(false);
        if (statement->table == nullptr)
            return nullptr;

        const TokenType action = this->Current().type;

        switch (action){
            case TokenType::Add:
            case TokenType::Drop:
            case TokenType::Alter:
            case TokenType::Rename:
                this->Advance();
                if (!this->Expect(TokenType::Column))
                    return nullptr;
                break;

            default:
                return this->Fail("expected ADD, DROP, ALTER or RENAME COLUMN");
        }

        if (action == TokenType::Add){
            statement->column.newColumn = this->ParseNewColumn();
            statement->type = Constants::AlterTableType::AddColumn;

            return (statement->column.newColumn == nullptr) ? nullptr : statement;
        }

        if (action == TokenType::Drop){
            auto* dropColumn = this->_allocator->Allocate<Statements::DropColumn>();
            if (!this->ParseColumnName(dropColumn->name))
                return nullptr;

            statement->column.dropColumn = dropColumn;
            statement->type = Constants::AlterTableType::DropColumn;

            return statement;
        }

        if (action == TokenType::Alter){
            auto* alterColumn = this->_allocator->Allocate<Statements::AlterColumn>();

            if (!this->ParseColumnName(alterColumn->name) || !this->ParseColumnType(alterColumn->type))
                return nullptr;

            statement->column.alterColumn = alterColumn;
            statement->type = Constants::AlterTableType::AlterColumn;

            return statement;
        }

        auto* renameColumn = this->_allocator->Allocate<Statements::RenameColumn>();

        if (!this->ParseColumnName(renameColumn->oldName))
            return nullptr;

        if (!this->Expect(TokenType::To))
            return nullptr;

        if (!this->ParseColumnName(renameColumn->newName))
            return nullptr;

        statement->column.renameColumn = renameColumn;
        statement->type = Constants::AlterTableType::RenameColumn;

        return statement;
    }

    Statements::Statement* SqlParser::ParseVariableDeclaration(const bool isDeclare){
        this->Advance(); //DECLARE or SET

        if (!this->Check(TokenType::Variable))
            return this->Fail("expected a variable name");

        auto name = this->MakeString(this->Advance());

        auto declaredType = DataType::Null;
        bool hasDeclaredType = false;

        if (IsDataTypeToken(this->Current().type)){
            ParsedDataType parsed;
            if (!this->ParseDataType(parsed))
                return nullptr;

            if (parsed.type == DataType::Json)
                return this->Fail("json is not a valid variable type");

            declaredType = parsed.type;
            hasDeclaredType = true;
        }

        Expressions::Expression* expression = nullptr;

        //DECLARE may omit the initialiser; SET always requires one.
        if (isDeclare ? this->Match(TokenType::Equal) : this->Expect(TokenType::Equal)){
            expression = this->ParseExpression();
            if (expression == nullptr)
                return nullptr;
        }
        else if (!isDeclare)
            return nullptr;

        auto nullValue = Value::Null();

        if (isDeclare){
            auto* statement = this->_allocator->Allocate<Statements::DeclareVariableStatement>();

            statement->variable.SetValue(nullValue);
            statement->variable.SetName(name);
            statement->expression = expression;
            statement->type = hasDeclaredType ? declaredType : statement->variable.GetType();

            return statement;
        }

        auto* statement = this->_allocator->Allocate<Statements::SetVariableStatement>();

        statement->variable.SetValue(nullValue);
        statement->variable.SetName(name);
        statement->expression = expression;
        statement->type = hasDeclaredType ? declaredType : statement->variable.GetType();

        return statement;
    }

    /**
     * @name Shared fragments
     * @{
     */

    bool SqlParser::ParseColumnName(Statements::ColumnName& column){
        if (this->Check(TokenType::Star)){
            column.name = this->MakeString(this->Advance());
            return true;
        }

        DataTypes::String first;
        if (!this->ParseIdentifier(first))
            return false;

        if (!this->Match(TokenType::Dot)){
            column.name = std::move(first);
            return true;
        }

        //ColumnName::alias is the table qualifier, not an output alias.
        column.alias = std::move(first);

        if (this->Check(TokenType::Star)){
            column.name = this->MakeString(this->Advance());
            return true;
        }

        return this->ParseIdentifier(column.name);
    }

    bool SqlParser::ParseColumnType(Statements::ColumnType& columnType){
        ParsedDataType parsed;
        if (!this->ParseDataType(parsed))
            return false;

        auto name = DataTypes::String(DataTypeName(parsed.type), this->_allocator);

        if (parsed.type == DataType::Decimal)
            columnType = Statements::ColumnType(name, Statements::DecimalType(parsed.precision, parsed.scale));
        else if (parsed.type == DataType::String)
            columnType = Statements::ColumnType(name, parsed.size);
        else
            columnType = Statements::ColumnType(name);

        return true;
    }

    bool SqlParser::ParseIdentifier(DataTypes::String& identifier){
        if (!CanBeIdentifier(this->Current().type)){
            this->Fail("expected an identifier");
            return false;
        }

        identifier = this->MakeString(this->Advance());
        return true;
    }

    bool SqlParser::ParseOptionalAlias(DataTypes::String& alias){
        //An explicit AS removes the ambiguity, so any name is fair game after it.
        if (this->Match(TokenType::As))
            return this->ParseIdentifier(alias);

        if (!CanBeIdentifier(this->Current().type) || SqlParser::IsClauseKeyword(this->Current().type))
            return true;

        alias = this->MakeString(this->Advance());
        return true;
    }

    bool SqlParser::ParseLiteralValue(Value& value){
        const bool negated = this->Check(TokenType::Minus);

        if (negated || this->Check(TokenType::Plus))
            this->Advance();

        switch (this->Current().type){
            case TokenType::StringLiteral:
            case TokenType::UnicodeStringLiteral: {
                auto text = this->MakeString(this->Advance());
                value = Value(text, this->_allocator, 0);
                return true;
            }

            case TokenType::IntegerLiteral:
            case TokenType::DecimalLiteral:
                return this->MakeNumericValue(this->Advance(), negated, value);

            case TokenType::True:
                this->Advance();
                value = Value(true, this->_allocator, 0);
                return true;

            case TokenType::False:
                this->Advance();
                value = Value(false, this->_allocator, 0);
                return true;

            case TokenType::Null:
                this->Advance();
                value = Value::Null(this->_allocator);
                return true;

            default:
                this->Fail("expected a literal value");
                return false;
        }
    }

    /** @} */
}
