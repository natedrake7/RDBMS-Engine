#pragma once
#include <string>
#include "../../AdditionalLibraries/HashSet/HashSet.h"

using namespace std;

namespace QueryParser 
{
    enum class WordType: uint8_t{
        Keyword = 0,
        Identifier = 1,
        Number = 2,
        String = 3,
        Symbol = 4,
        WildCard = 5,
        Uknown = 6
    };

    typedef struct Token{
        string value;
        WordType type;
    }Token;

    static HashSet<string> keywordsHashSet = 
    {
        "SELECT", 
        "FROM", 
        "WHERE", 
        "GROUP BY", 
        "HAVING", 
        "ORDER BY", 
        "ASC", 
        "DESC"
    };

    vector<Token> TokenizeQuery(const string& query);
}
