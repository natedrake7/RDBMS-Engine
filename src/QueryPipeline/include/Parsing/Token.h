#pragma once
#include "../../../Systemic/include/DataTypes/StringView.h"

namespace QueryPipeline::Parsing{

    /**
     * All token kinds produced by the hand written SQL lexer.
     *
     * The keyword block must stay contiguous between FirstKeyword and LastKeyword,
     * so keyword membership is a single range check instead of a table lookup.
     */
    enum class TokenType : UnsignedSmallInt {
        EndOfFile = 0,
        Invalid,

        //Literals and names
        Identifier,
        Variable,
        IntegerLiteral,
        DecimalLiteral,
        StringLiteral,
        UnicodeStringLiteral,

        //Punctuation
        LeftParen,
        RightParen,
        Comma,
        Semicolon,
        Dot,

        //Arithmetic operators
        Plus,
        Minus,
        Star,
        Slash,
        Percent,

        //Comparison operators
        Equal,
        NotEqual,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,

        //Json accessors
        JsonObjectAccessor, // ->
        JsonScalarAccessor, // ->>

        //Keywords. Keep contiguous, see FirstKeyword / LastKeyword below.
        Select,
        From,
        Where,
        Order,
        By,
        Values,
        Into,
        Database,
        Use,
        Create,
        Drop,
        Insert,
        Delete,
        Update,
        On,
        Table,
        Default,
        To,
        Top,
        Distinct,
        With,
        Password,
        Role,
        Grant,
        User,
        Schema,

        Declare,
        Set,

        Alter,
        Rename,
        Add,
        Column,

        Unique,
        Index,
        Primary,
        Key,
        Identity,
        Constraint,

        Count,
        Sum,
        Avg,
        Min,
        Max,

        Not,
        Null,

        Desc,
        Asc,

        Left,
        Right,
        Full,
        Inner,
        Outer,
        Join,

        As,

        And,
        Or,

        Switch,
        Case,
        When,
        Then,
        Iif,

        Cast,
        TryCast,

        True,
        False,

        Bool,
        TinyInt,
        SmallInt,
        Int,
        BigInt,
        Decimal,
        DateTime,
        Guid,
        Json,
        String,

        FirstKeyword = Select,
        LastKeyword = String,
    };

    /**
     * A single lexeme.
     *
     * @c Text points straight into the query buffer handed to the lexer, so a token
     * costs nothing to produce and stays valid only for as long as that buffer does.
     * Anything that outlives the parse must be copied into the compilation arena.
     *
     * @c Text carries the semantic content rather than the raw source span: quotes are
     * stripped from string literals and brackets from quoted identifiers. Escape
     * sequences inside string literals are left untouched for the parser to resolve.
     */
    struct Token {
        const char* text;
        Int length;

        UnsignedInt line;   //1 based
        UnsignedInt column; //1 based

        TokenType type;

        [[nodiscard]] DataTypes::StringView View() const{
            return DataTypes::StringView(this->text, this->length);
        }
    };

    [[nodiscard]] constexpr bool IsKeyword(const TokenType type){
        return type >= TokenType::FirstKeyword && type <= TokenType::LastKeyword;
    }

    /**
     * True for keywords that may still be used as an identifier, mirroring the
     * reservedAsIdentifier rule in SQL.g4. A recursive descent parser resolves these
     * from context, so no grammar gymnastics are needed.
     */
    [[nodiscard]] bool CanBeIdentifier(TokenType type);

    /**
     * Human readable name for a token type, used to build syntax error messages.
     * Keywords and operators render as the source text they match ("SELECT", "<=").
     */
    [[nodiscard]] DataTypes::StringView TokenTypeName(TokenType type);
}
