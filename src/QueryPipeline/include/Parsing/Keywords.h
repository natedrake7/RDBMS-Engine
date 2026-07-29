#pragma once
#include "Token.h"
#include "../../../Systemic/include/DataStructures/ConstexprDictionary.h"

namespace QueryPipeline::Parsing{

    /**
     * Every SQL keyword, keyed by its lower cased spelling. The dictionary hashes and
     * compares case insensitively, so the lexer can probe it with a view straight over
     * the query buffer without lower casing anything first.
     */
    static constexpr ConstexprDictionary KEYWORDS{
        Pair(DataTypes::StringView("select"),     TokenType::Select),
        Pair(DataTypes::StringView("from"),       TokenType::From),
        Pair(DataTypes::StringView("where"),      TokenType::Where),
        Pair(DataTypes::StringView("order"),      TokenType::Order),
        Pair(DataTypes::StringView("by"),         TokenType::By),
        Pair(DataTypes::StringView("values"),     TokenType::Values),
        Pair(DataTypes::StringView("into"),       TokenType::Into),
        Pair(DataTypes::StringView("database"),   TokenType::Database),
        Pair(DataTypes::StringView("use"),        TokenType::Use),
        Pair(DataTypes::StringView("create"),     TokenType::Create),
        Pair(DataTypes::StringView("drop"),       TokenType::Drop),
        Pair(DataTypes::StringView("insert"),     TokenType::Insert),
        Pair(DataTypes::StringView("delete"),     TokenType::Delete),
        Pair(DataTypes::StringView("update"),     TokenType::Update),
        Pair(DataTypes::StringView("on"),         TokenType::On),
        Pair(DataTypes::StringView("table"),      TokenType::Table),
        Pair(DataTypes::StringView("default"),    TokenType::Default),
        Pair(DataTypes::StringView("to"),         TokenType::To),
        Pair(DataTypes::StringView("top"),        TokenType::Top),
        Pair(DataTypes::StringView("distinct"),   TokenType::Distinct),
        Pair(DataTypes::StringView("with"),       TokenType::With),
        Pair(DataTypes::StringView("password"),   TokenType::Password),
        Pair(DataTypes::StringView("role"),       TokenType::Role),
        Pair(DataTypes::StringView("grant"),      TokenType::Grant),
        Pair(DataTypes::StringView("user"),       TokenType::User),
        Pair(DataTypes::StringView("schema"),     TokenType::Schema),

        Pair(DataTypes::StringView("declare"),    TokenType::Declare),
        Pair(DataTypes::StringView("set"),        TokenType::Set),

        Pair(DataTypes::StringView("alter"),      TokenType::Alter),
        Pair(DataTypes::StringView("rename"),     TokenType::Rename),
        Pair(DataTypes::StringView("add"),        TokenType::Add),
        Pair(DataTypes::StringView("column"),     TokenType::Column),

        Pair(DataTypes::StringView("unique"),     TokenType::Unique),
        Pair(DataTypes::StringView("index"),      TokenType::Index),
        Pair(DataTypes::StringView("primary"),    TokenType::Primary),
        Pair(DataTypes::StringView("key"),        TokenType::Key),
        Pair(DataTypes::StringView("identity"),   TokenType::Identity),
        Pair(DataTypes::StringView("constraint"), TokenType::Constraint),

        Pair(DataTypes::StringView("count"),      TokenType::Count),
        Pair(DataTypes::StringView("sum"),        TokenType::Sum),
        Pair(DataTypes::StringView("avg"),        TokenType::Avg),
        Pair(DataTypes::StringView("min"),        TokenType::Min),
        Pair(DataTypes::StringView("max"),        TokenType::Max),

        Pair(DataTypes::StringView("not"),        TokenType::Not),
        Pair(DataTypes::StringView("null"),       TokenType::Null),

        Pair(DataTypes::StringView("desc"),       TokenType::Desc),
        Pair(DataTypes::StringView("asc"),        TokenType::Asc),

        Pair(DataTypes::StringView("left"),       TokenType::Left),
        Pair(DataTypes::StringView("right"),      TokenType::Right),
        Pair(DataTypes::StringView("full"),       TokenType::Full),
        Pair(DataTypes::StringView("inner"),      TokenType::Inner),
        Pair(DataTypes::StringView("outer"),      TokenType::Outer),
        Pair(DataTypes::StringView("join"),       TokenType::Join),

        Pair(DataTypes::StringView("as"),         TokenType::As),

        Pair(DataTypes::StringView("and"),        TokenType::And),
        Pair(DataTypes::StringView("or"),         TokenType::Or),

        Pair(DataTypes::StringView("switch"),     TokenType::Switch),
        Pair(DataTypes::StringView("case"),       TokenType::Case),
        Pair(DataTypes::StringView("when"),       TokenType::When),
        Pair(DataTypes::StringView("then"),       TokenType::Then),
        Pair(DataTypes::StringView("iif"),        TokenType::Iif),

        Pair(DataTypes::StringView("cast"),       TokenType::Cast),
        Pair(DataTypes::StringView("try_cast"),   TokenType::TryCast),

        Pair(DataTypes::StringView("true"),       TokenType::True),
        Pair(DataTypes::StringView("false"),      TokenType::False),

        Pair(DataTypes::StringView("bool"),       TokenType::Bool),
        Pair(DataTypes::StringView("tinyint"),    TokenType::TinyInt),
        Pair(DataTypes::StringView("smallint"),   TokenType::SmallInt),
        Pair(DataTypes::StringView("int"),        TokenType::Int),
        Pair(DataTypes::StringView("bigint"),     TokenType::BigInt),
        Pair(DataTypes::StringView("decimal"),    TokenType::Decimal),
        Pair(DataTypes::StringView("datetime"),   TokenType::DateTime),
        Pair(DataTypes::StringView("guid"),       TokenType::Guid),
        Pair(DataTypes::StringView("json"),       TokenType::Json),
        Pair(DataTypes::StringView("string"),     TokenType::String),
    };

    static constexpr Int MIN_KEYWORD_LENGTH = 2;  //"as", "by", "on", "or", "to"
    static constexpr Int MAX_KEYWORD_LENGTH = 10; //"constraint"

    /**
     * Classifies an identifier shaped lexeme. Returns TokenType::Identifier when the
     * text is not a keyword.
     */
    [[nodiscard]] inline TokenType LookupKeyword(const DataTypes::StringView& text){
        if (text.Size() < MIN_KEYWORD_LENGTH || text.Size() > MAX_KEYWORD_LENGTH)
            return TokenType::Identifier;

        auto type = TokenType::Identifier;
        return KEYWORDS.TryGetValue(text, type)
                   ? type
                   : TokenType::Identifier;
    }
}
