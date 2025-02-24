#pragma once
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
        ScalarVariable = 7
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
        As = 14
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
        {"AS", KeyWord::As}
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

        public:
            static Tokenizer& Get()
            {
                static Tokenizer instance;

                return instance;
            }
            static vector<Token> TokenizeQuery(const string& query);

    };

}
