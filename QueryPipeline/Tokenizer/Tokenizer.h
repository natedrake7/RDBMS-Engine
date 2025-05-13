#pragma once
#include <regex>
#include <string>
#include "../../AdditionalLibraries/Dictionary/Dictionary.h"
#include "../../AdditionalLibraries/HashSet/HashSet.h"

using namespace std;

namespace QueryParser 
{
    enum class WordType: uint8_t{
        Uknown = 0,
        Keyword = 1,
        Identifier = 2,
        Number = 3,
        String = 4,
        Symbol = 5,
        WildCard = 6,
        ScalarVariable = 7,
        AggregateFunction = 8,
        WhiteSpace = 9,
        Comment = 10,
        Program = 11,
        Semicolon = 12,
        LeftParenthesis = 13,
        RightParenthesis = 14
    };

    typedef struct ReguralExpressions{
        regex expression;
        WordType type;
    }ReguralExpressions;

    static string SQLKeywords[] = {
        "SELECT",
        "FROM",
        "WHERE",
        "ON",
        "LEFT\\s+JOIN",
        "RIGHT\\s+JOIN",
        "INNER\\s+JOIN",
        "OUTER\\s+JOIN",
        "FULL\\s+JOIN"
    };

    typedef struct Token{
        string value;
        WordType type;
    }Token;

    enum class KeyWord: uint8_t{
        Select = 1,
        Insert = 2,
        Update = 3,
        Delete = 4,
        From = 5,
        Where = 6,
        Group = 7,
        By = 8,
        Having = 9,
        Order = 10,
        Asc = 11,
        Desc = 12,
        Into = 13,
        As = 14,
        Sum = 15,
        Avg = 16,
        Min = 17,
        Max = 18,
        Count = 19,
        Inner = 20,
        Join = 21,
        InnerJoin = 22,
        On = 23,
        LeftJoin = 23,
        RightJoin = 24,
        FullJoin = 25,
        Left = 26,
        Right = 27,
        Full = 28,
    };

    static Dictionary<string, KeyWord> keywordsDictionary = {
        {"SELECT", KeyWord::Select},
        {"INSERT", KeyWord::Insert},
        {"UPDATE", KeyWord::Update},
        {"DELETE", KeyWord::Delete},
        {"FROM", KeyWord::From},
        {"WHERE", KeyWord::Where},
        {"GROUP", KeyWord::Group},
        {"BY", KeyWord::By},
        {"HAVING", KeyWord::Having},
        {"ORDER", KeyWord::Order},
        {"ASC", KeyWord::Asc},
        {"DESC", KeyWord::Desc},
        {"INTO", KeyWord::Into},
        {"AS", KeyWord::As},
    };

    static Dictionary<string, KeyWord> aggregateKeywordsDictionary = {
        {"SUM", KeyWord::Sum},
        {"AVG", KeyWord::Avg},
        {"MIN", KeyWord::Min},
        {"MAX", KeyWord::Max},
        {"COUNT", KeyWord::Count}
    };

    static Dictionary<string, KeyWord> joinsKeywordsDictionary = {
        {"INNER", KeyWord::Inner},
        {"LEFT", KeyWord::Left},
        {"RIGHT", KeyWord::Right},
        {"FULL", KeyWord::Full},
        {"JOIN", KeyWord::Join},
        {"ON", KeyWord::On}
    };

    static HashSet<char> symbolsHashSet = 
    {
        '>', 
        '<', 
        '=', 
        '!',
        '>',
        '<',
        '+',
        '-',
        '*',
        '/', 
        ',', 
        ';'
    };

    constexpr bool ValidateScalarVariableStructure(const char& c, bool isFirstChar);
    constexpr bool ValidateMultiCharacterOperations(const char& c, const string& query, const int& i);
    constexpr bool ValidateAlphabeticCharacters(const char& c);

    class Tokenizer{

        Tokenizer();
        ~Tokenizer();

        static string& BuildKeywordsRegex();

        public:
            static Tokenizer& Get()
            {
                static Tokenizer instance;

                return instance;
            }
            static vector<Token> TokenizeQuery(const string& query);
    };

}
