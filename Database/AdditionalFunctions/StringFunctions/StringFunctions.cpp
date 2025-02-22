#include "StringFunctions.h"

namespace StringFunctions {
    string Concat(const vector<string>& strings)
    {
        string result;
        for(const auto& string : strings)
            result.append(string);

        return result;
    }

    int Ascii(const string &str)
    {
        if(str.empty())
            return 0;

        return str.front();
    }

    string Char(const int &asciiCode) { return {0, static_cast<char>(asciiCode)}; }

    int CharIndex(const string &subStr, const string &str, const int &startIndex)
    {
        if(subStr.empty()
            || str.empty()
            || startIndex < 0
            || startIndex > str.size()
            || subStr.size() > str.size())
            return 0;

        return static_cast<int>(str.find(subStr, startIndex));
    }

    int DataLength(const string &str) { return static_cast<int>(str.size()); }

    string Left(const string &str, const int &numberOfCharacters)
    {
        if(str.empty() || numberOfCharacters <= 0)
            return str;

        return str.substr(0, numberOfCharacters);
    }

    string Right(const string &str, const int &numberOfCharacters)
    {
        if(str.empty() || numberOfCharacters <= 0)
            return str;

        return str.substr(str.size() - numberOfCharacters, numberOfCharacters);
    }

    int Length(const string &str)
    {
        if(str.empty())
            return 0;

        return static_cast<int>(Trim(str).size());
    }

    string Lower(const string &str)
    {
        if(str.empty())
            return str;

        string result;

        for(const auto& character : str)
            result += static_cast<char>(tolower(character));

        return result;
    }

    string Upper(const string &str)
    {
        if(str.empty())
            return str;

        string result;

        for(const auto& character : str)
            result += static_cast<char>(toupper(character));

        return result;
    }

    string Trim(const string &str)
    {
        if(str.empty())
            return str;

        int firstIndex = 0;
        int lastIndex = str.size() - 1;

        for(int i = 0;i < str.size(); i++)
            if(!isspace(str[i]))
            {
                firstIndex = i;
                break;
            }

        for(int i = str.size() - 1; i >= 0; i--)
            if(!isspace(str[i]))
            {
                lastIndex = i;
                break;
            }

        return str.substr(firstIndex, lastIndex - firstIndex + 1);
    }

    string TrimLeft(const string &str)
    {
        if(str.empty())
            return str;

        int firstIndex = 0;

        for(int i = 0;i < str.size(); i++)
            if(!isspace(str[i]))
            {
                firstIndex = i;
                break;
            }

        return str.substr(firstIndex);
    }

    string TrimRight(const string &str)
    {
        if(str.empty())
            return str;

        int lastIndex = str.size() - 1;
        
        for(int i = str.size() - 1; i >= 0; i--)
            if(!isspace(str[i]))
            {
                lastIndex = i;
                break;
            }

        return str.substr(0, lastIndex);
    }

    string Replace(const string &str, const string &subStr, const string &replaceStr)
    {
        if(str.empty() || subStr.empty())
            return str;

        string result(str);
        int subStrIndex = 0;

        while (true)
        {
            subStrIndex = static_cast<int>(result.find(subStr));

            if(subStrIndex == string::npos)
                break;

            result.replace(subStrIndex, subStr.size(), replaceStr);
        }
        
        return result;
    }

    string SubString(const string &str, const int &startIndex, const int &endIndex)
    {
        if(startIndex > str.size() -1 || endIndex > str.size() - 1)
            return str;

        return str.substr(startIndex, endIndex - startIndex + 1);
    }
}


