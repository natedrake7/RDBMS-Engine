#include "Tokenizer.h"
#include <cctype>
#include <stdexcept>
#include <vector>

namespace QueryParser 
{
    Tokenizer::Tokenizer() = default;

    Tokenizer::~Tokenizer() = default;

    constexpr bool ValidateScalarVariableStructure(const char& c, bool isFirstChar) 
    {
        return isFirstChar 
                ? (isalpha(c) || c == '_') 
                : (isalnum(c) || c == '_');
    }

    constexpr bool ValidateMultiCharacterOperations(const char& c, const string& query, const int& i)
    {
        return (c == '>' || c == '<' || c == '!' || c == '=') && 
                (i + 1 < query.size() && query[i + 1] == '=');
    }

    constexpr bool ValidateAlphabeticCharacters(const char& c)
    {
        return isalnum(c) || c == '_';
    }

    vector<Token> Tokenizer::TokenizeQuery(const string& query)
    {
        vector<Token> tokens;
        string buffer;
        size_t i = 0;

        while (i < query.size()) 
        {
            buffer.clear();

            const char& c = query[i];

            // Handle Alphabetic Characters (Keywords / Identifiers)
            if (isalpha(c)) 
            {
                while (i < query.size() && ValidateAlphabeticCharacters(query[i])) 
                    buffer += query[i++];

                if (keywordsDictionary.Contains(buffer))
                    tokens.push_back({buffer, WordType::Keyword});
                else
                    tokens.push_back({buffer, WordType::Identifier});

                continue;
            }

            // Handle Numbers
            if (isdigit(c)) 
            {
                while (i < query.size() && isdigit(query[i])) 
                    buffer += query[i++];

                tokens.push_back({buffer, WordType::Number});

                continue;
            }

            // Handle Symbols (e.g., >, >=, <=, =, !=, , , ;)
            if (symbolsHashSet.Contains(c))
            {
                //handle operations like  != , >= etc
                if (ValidateMultiCharacterOperations(c, query, i)) 
                {
                    tokens.push_back({{string(1, c) + "="}, WordType::Symbol});  // Handle >=, <=, !=
                    i += 2;
                    continue;
                } 

                tokens.push_back({string(1, c), WordType::Symbol});
                i++;
                continue;
            }

            // Ignore Whitespace
            if (isspace(c)) 
            {
                i++;
                continue;
            }

            //handle wildcard characters
            if(c == '*')
            {
                tokens.push_back({string(1, c), WordType::WildCard});
                i++;
                continue;
            }

            //handle scalar variables
            if(c == '@')
            {
                if (i == query.size() || !isalpha(query[i + 1])) 
                    throw std::runtime_error("Tokenizer::TokenizeQuery: Invalid scalar variable name!");

                
                buffer += query[i++];
                const int firstCharacterIndex = i;

                while (i < query.size() && ValidateScalarVariableStructure(query[i], firstCharacterIndex == i))
                    buffer += query[i++];

                tokens.push_back({buffer, WordType::ScalarVariable});

                continue;
            }

            //handle strings
            if (c == '\'') 
            {
                i++;
                bool isQuoteMarkEnclosed = false;
            
                while (i < query.size()) 
                {
                    if (query[i] == '\'') 
                    {
                        if (i + 1 < query.size() && query[i + 1] == '\'') 
                        {
                            buffer += '\'';
                            i += 2;
                            continue;
                        }

                        i++;  // Closing quote found
                        isQuoteMarkEnclosed = true;
                        break;
                    }
                    buffer += query[i++];
                }
            
                if (!isQuoteMarkEnclosed)
                    throw std::runtime_error("Tokenizer::TokenizeQuery: String not properly enclosed with single quotes!");
            
                tokens.push_back({buffer, WordType::String});
                continue;
            }

            // Unknown character (error handling)
            // tokens.push_back({string(1, c), WordType::Uknown});
            // i++;

            throw runtime_error("Tokenizer::TokenizeQuery: Unexpected character '" 
                        + string(1, c) +
                         "' at position " + to_string(i));
        }

        return tokens;
    }
}