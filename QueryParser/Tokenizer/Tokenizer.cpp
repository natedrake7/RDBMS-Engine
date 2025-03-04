#include "Tokenizer.h"
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>

namespace QueryParser 
{
    vector<ReguralExpressions> regexPatterns = {
        {regex(R"(\s+)"), WordType::WhiteSpace} ,        // Punctuation
        {regex(R"(#.*)"), WordType::Comment} ,   
        {regex(R"(/\*[\s\S]*?\*/)"), WordType::Comment},
        {regex(R"(@\w+)"), WordType::ScalarVariable},  // Scalar variable (e.g., @variable)
        {regex(R"(\d+)"), WordType::Number},           // Numbers
        {regex(R"('[^']*')"), WordType::String},       // Strings enclosed in single quotes
        {regex(R"([a-zA-Z_]\w*)"), WordType::Identifier}, // Identifiers (table/column names)
        {regex(R"([<>!=]=?)"), WordType::Symbol},      // Operators (>=, <=, !=, etc.)
        {regex(R"(\*)"), WordType::WildCard},          // Wildcard (*)
        {regex(R"([,;()])"), WordType::Symbol}        // Punctuation
    };

    Tokenizer::Tokenizer()
    {
        const string& keywordsRegexExp = Tokenizer::BuildKeywordsRegex();

        regexPatterns.insert(regexPatterns.begin() + 1, {regex(keywordsRegexExp), WordType::Keyword});
    }

    Tokenizer::~Tokenizer() = default;
    
    string& Tokenizer::BuildKeywordsRegex()
    {
        static string regexp;

        regexp += R"(\b()";
        for (const auto& keyword : SQLKeywords)
            regexp += keyword + "|";

        regexp = regexp.substr(0, regexp.size() - 1);  // Remove the last pipe "|"
        regexp += R"()\b)";
        return regexp;
    }

    vector<Token> Tokenizer::TokenizeQuery(const string& query)
    {
        vector<Token> tokens;
        string remainingQuery = query;
        smatch match;

        while (!remainingQuery.empty()) 
        {
            bool matched = false;

            // Try matching each regex pattern
            for (const auto& [pattern, type] : regexPatterns) 
            {
                if (!regex_search(remainingQuery, match, pattern,regex_constants::match_continuous)) 
                    continue;

                matched = true;
                if(type == WordType::WhiteSpace || type == WordType::Comment)
                {
                    remainingQuery = remainingQuery.substr(match.length(0)); // Move past the matched token
                    break;
                }
                
                tokens.push_back({match.str(), type});
                remainingQuery = remainingQuery.substr(match.length(0)); // Move past the matched token

                break;
            }

            if(!matched)
                throw runtime_error("Tokenizer Error: Unexpected character at: " + remainingQuery);
        }

        return tokens;
    }
}