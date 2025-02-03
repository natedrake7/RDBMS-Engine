#pragma once
#include <string>
#include <unordered_set>

using namespace std;

namespace QueryParser {
    enum class WordType: uint8_t{
        Keyword = 0,
        Identifier = 1,
        Number = 2,
        String = 3,
        Symbol = 4,
        Uknown = 5
    };

    typedef struct Token{
        string value;
        WordType type;
    }Token;

    static unordered_set<string> keywords = {
        "SELECT",
        "FROM",
        "ORDER BY",
        "GROUP BY",
        "WHERE",
        "IN",
        "UPDATE",
        "INSERT INTO"
    };

    vector<Token> TokenizeQuery(const string& query);
}
