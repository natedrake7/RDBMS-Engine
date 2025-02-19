#include "Tokenizer.h"
#include <vector>

namespace QueryParser 
{
    vector<Token> TokenizeQuery(const string& query)
    {
        vector<Token> tokens;
        string buffer;
        size_t i = 0;

        while (i < query.size()) {
            char c = query[i];

                // Handle Alphabetic Characters (Keywords / Identifiers)
                if (isalpha(c)) {
                    buffer.clear();
                    while (i < query.size() && (isalpha(query[i]) || isdigit(query[i]) || query[i] == '_')) {
                        buffer += query[i++];
                    }
                    if (keywords.count(buffer))
                        tokens.push_back({buffer, WordType::Keyword});
                    else
                        tokens.push_back({buffer, WordType::Identifier});
                    continue;
                }

                // Handle Numbers
                if (isdigit(c)) {
                    buffer.clear();
                    while (i < query.size() && isdigit(query[i])) {
                        buffer += query[i++];
                    }
                    tokens.push_back({buffer, WordType::Number});
                    continue;
                }

                // Handle Symbols (e.g., >, >=, <=, =, !=, , , ;)
                if (c == '>' || c == '<' || c == '=' || c == '!' || c == ',' || c == ';') {
                    if ((c == '>' || c == '<' || c == '!') && i + 1 < query.size() && query[i + 1] == '=') {
                        tokens.push_back({string(1, c) + "=", WordType::Symbol});  // Handle >=, <=, !=
                        i += 2;
                    } else {
                        tokens.push_back({string(1, c), WordType::Symbol});
                        i++;
                    }
                    continue;
                }

                // Ignore Whitespace
                if (isspace(c)) {
                    i++;
                    continue;
                }

                if(c == '*')
                {
                    tokens.push_back({string(1, c), WordType::WildCard});
                    i++;
                    continue;
                }

                // Unknown character (error handling)
                tokens.push_back({string(1, c), WordType::Uknown});
                i++;
            }

            return tokens;
    }
}