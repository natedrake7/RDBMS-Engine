#pragma once
#include "Token.h"
#include "Diagnostic.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

class Value;

namespace Memory{
    class IAllocator;
}

namespace Expressions{
    class Expression;
    class ColumnExpression;
}

namespace QueryPipeline::Statements{
    struct Statement;
    struct SelectStatement;
    struct JoinStatement;
    struct DataSource;
    struct ColumnName;
    struct NewColumn;
    struct Identity;
    struct PrimaryKeyConstraint;
    struct OrderByStatement;
    struct OrderColumn;
    struct WhereClause;
    struct UpdateColumn;
    struct ColumnType;
}

namespace QueryPipeline::Parsing{

    /**
     * A parsed type reference. CAST only cares about @c type, while CREATE TABLE also
     * needs the width of STRING(n) and the precision/scale of DECIMAL(p,s).
     */
    struct ParsedDataType {
        DataType type;

        Int size;        //STRING(n); MAX_STRING_SIZE for STRING(MAX), 0 when not a string
        TinyInt precision;
        TinyInt scale;

        ParsedDataType()
            : type(DataType::Null), size(0), precision(0), scale(0){}
    };

    static constexpr Int MAX_STRING_SIZE = -1;

    /**
     * Recursive descent parser over a token buffer, building Expressions::* nodes
     * straight into the compilation arena. There is no intermediate parse tree and no
     * type erasure: a node is constructed once, in place, and handed back.
     *
     * Nothing here throws. Every Parse* returns nullptr once an error has been recorded
     * in the diagnostic, so failures unwind by propagating nullptr.
     *
     * Operator precedence, loosest to tightest, matching the expression rule in SQL.g4:
     *   OR < AND < NOT < (= !=) < (< <= > >=) < (+ -) < (* / %) < unary +/- < postfix -> ->>
     */
    class SqlParser final{
        const Token* _tokens;
        const ::Memory::IAllocator* _allocator;
        Diagnostic* _diagnostic;

        Int _count;
        Int _position;

        //Cursor
        [[nodiscard]] const Token& Peek(Int offset = 0) const;
        [[nodiscard]] bool Check(TokenType type) const;
        const Token& Advance();
        bool Match(TokenType type);
        bool Expect(TokenType type);

        //Returns nullptr so an expression rule can bail out with `return this->Fail(...)`.
        std::nullptr_t Fail(const DataTypes::StringView& message) const;

        //Precedence cascade
        [[nodiscard]] Expressions::Expression* ParseOr();
        [[nodiscard]] Expressions::Expression* ParseAnd();
        [[nodiscard]] Expressions::Expression* ParseNot();
        [[nodiscard]] Expressions::Expression* ParseEquality();
        [[nodiscard]] Expressions::Expression* ParseRelational();
        [[nodiscard]] Expressions::Expression* ParseAdditive();
        [[nodiscard]] Expressions::Expression* ParseMultiplicative();
        [[nodiscard]] Expressions::Expression* ParseUnary();
        [[nodiscard]] Expressions::Expression* ParsePostfix();
        [[nodiscard]] Expressions::Expression* ParsePrimary();

        //Primaries
        [[nodiscard]] Expressions::Expression* ParseLiteral();
        [[nodiscard]] Expressions::Expression* ParseNumericLiteral(bool negated);
        [[nodiscard]] Expressions::Expression* ParseVariable();
        [[nodiscard]] Expressions::Expression* ParseColumnReference();
        [[nodiscard]] Expressions::Expression* ParseFunctionCall(const Token& nameToken);
        [[nodiscard]] Expressions::Expression* ParseCast();
        [[nodiscard]] Expressions::Expression* ParseSwitch();
        [[nodiscard]] Expressions::Expression* ParseIif();

        [[nodiscard]] Expressions::Expression* ApplyJsonAccessors(Expressions::ColumnExpression* column);
        [[nodiscard]] bool ParseJsonKey(DataTypes::String& key);

        //Shared by expression literals and by DEFAULT clauses.
        [[nodiscard]] bool MakeNumericValue(const Token& token, bool negated, Value& value) const;

        [[nodiscard]] bool IsFunctionCallAhead() const;

        //Statements
        [[nodiscard]] Statements::Statement* ParseSelect();
        [[nodiscard]] Statements::Statement* ParseInsert();
        [[nodiscard]] Statements::Statement* ParseUpdate();
        [[nodiscard]] Statements::Statement* ParseDelete();
        [[nodiscard]] Statements::Statement* ParseCreate();
        [[nodiscard]] Statements::Statement* ParseCreateTable();
        [[nodiscard]] Statements::Statement* ParseCreateIndex(bool isUnique);
        [[nodiscard]] Statements::Statement* ParseCreateSchema();
        [[nodiscard]] Statements::Statement* ParseCreateDatabase();
        [[nodiscard]] Statements::Statement* ParseCreateUser();
        [[nodiscard]] Statements::Statement* ParseDropDatabase();
        [[nodiscard]] Statements::Statement* ParseUseDatabase();
        [[nodiscard]] Statements::Statement* ParseGrantRole();
        [[nodiscard]] Statements::Statement* ParseAlterTable();
        [[nodiscard]] Statements::Statement* ParseVariableDeclaration(bool isDeclare);

        //Statement fragments
        /**
         * @param allowAlias false for the DDL forms (CREATE/ALTER TABLE, CREATE INDEX
         * ... ON, INSERT INTO) where no alias is meaningful. Without this the name would
         * swallow the keyword that follows it: ADD, DROP, ALTER and RENAME are all
         * non reserved, so `ALTER TABLE t ADD COLUMN ...` reads ADD as t's alias.
         */
        [[nodiscard]] Statements::DataSource* ParseTableName(bool allowAlias);
        [[nodiscard]] Statements::DataSource* ParseDataSource();
        [[nodiscard]] Statements::JoinStatement* ParseJoin();
        [[nodiscard]] Statements::OrderByStatement* ParseOrderBy();
        [[nodiscard]] Statements::NewColumn* ParseNewColumn();
        [[nodiscard]] Statements::PrimaryKeyConstraint* ParsePrimaryKeyConstraint();
        [[nodiscard]] Statements::Identity* ParseIdentity();

        [[nodiscard]] bool ParseSelectItem(Expressions::Expression*& expression);
        [[nodiscard]] bool ParseWhereClause(Statements::WhereClause& where);
        [[nodiscard]] bool ParseTop(BigInt& top);
        [[nodiscard]] bool ParseColumnName(Statements::ColumnName& column);
        [[nodiscard]] bool ParseColumnType(Statements::ColumnType& columnType);
        [[nodiscard]] bool ParseIdentifier(DataTypes::String& identifier);
        [[nodiscard]] bool ParseOptionalAlias(DataTypes::String& alias);
        [[nodiscard]] bool ParseLiteralValue(Value& value);

        /**
         * Keywords that open a clause and so must not be swallowed as a bare alias.
         * They are all non reserved, which is exactly why the check has to be positional:
         * `FROM orders JOIN ...` must not read JOIN as the alias of orders.
         */
        [[nodiscard]] static bool IsClauseKeyword(TokenType type);

        public:
            SqlParser(
                const Token* tokens,
                Int count,
                const ::Memory::IAllocator* allocator,
                Diagnostic& diagnostic
            );
            SqlParser(
                const DataStructures::PolymorphicArray<Token>& tokens,
                const ::Memory::IAllocator* allocator,
                Diagnostic& diagnostic
            );

            /**
             * Parses one complete expression. Returns nullptr and records a diagnostic
             * on malformed input.
             */
            [[nodiscard]] Expressions::Expression* ParseExpression();

            /**
             * Parses a whole query: one or more statements, each terminated by ';',
             * appending them to @p statements.
             *
             * @return false if a syntax error was recorded in the diagnostic. Statements
             * parsed before the failure are still in the array, but the caller should
             * treat the batch as failed.
             */
            [[nodiscard]] bool ParseStatements(
                DataStructures::PolymorphicArray<Statements::Statement*>* statements,
                const DataTypes::Guid* sessionId,
                Int databaseId
            );

            /**
             * Parses a single statement, without its terminating ';'.
             */
            [[nodiscard]] Statements::Statement* ParseStatement(const DataTypes::Guid* sessionId, Int databaseId);

            /**
             * Parses a type reference: BOOL, INT, STRING(n), STRING(MAX), DECIMAL(p,s),
             * and the rest of the dataType rule.
             */
            [[nodiscard]] bool ParseDataType(ParsedDataType& result);

            /**
             * Copies a token's text into the arena. Identifiers and string literals are
             * borrowed views until this point.
             */
            [[nodiscard]] DataTypes::String MakeString(const Token& token) const;

            [[nodiscard]] const Token& Current() const;
            [[nodiscard]] bool AtEnd() const;
            [[nodiscard]] bool HasError() const;
    };
}
