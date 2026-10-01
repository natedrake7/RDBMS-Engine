#pragma once
#include "Token.h"
#include "../../../Systemic/include/DataStructures/ConstexprDictionary.h"

namespace QueryPipeline::Parsing{

    inline constexpr  std::size_t FIRST_KEYWORD = static_cast<std::size_t>(TokenType::FirstKeyword);
    inline constexpr  std::size_t LAST_KEYWORD = static_cast<std::size_t>(TokenType::LastKeyword);
    inline constexpr std::size_t KEYWORD_COUNT = TOKEN_TYPE_COUNT - FIRST_KEYWORD;

    /**
    * Every SQL keyword, generated from the TokenType keyword block. The dictionary hashes
    * and compares case insensitively, so keys keep the enumerator's casing ("Select") and
    * the lexer can probe it with a view straight over the query buffer.
    */

    static constexpr auto KEYWORDS = []<std::size_t... I>(std::index_sequence<I...>){
        return ConstexprDictionary<DataTypes::StringView, TokenType, KEYWORD_COUNT>{
            Pair(TokenSourceText(static_cast<TokenType>(FIRST_KEYWORD + I)),
                static_cast<TokenType>(FIRST_KEYWORD + I)
            )...
        };
    }(std::make_index_sequence<KEYWORD_COUNT>{});

    static constexpr data_size_t MIN_KEYWORD_LENGTH = []{
        auto length = std::numeric_limits<data_size_t>::max();
        for (auto i = FIRST_KEYWORD; i < TOKEN_TYPE_COUNT; ++i)
            length = std::min(length, TokenSourceText(static_cast<TokenType>(i)).Size());
        return length;
    }();

    static constexpr data_size_t MAX_KEYWORD_LENGTH = []{
        data_size_t length = 0;
        for (auto i = FIRST_KEYWORD; i < TOKEN_TYPE_COUNT; ++i)
            length = std::max(length, TokenSourceText(static_cast<TokenType>(i)).Size());
        return length;
    }();

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
