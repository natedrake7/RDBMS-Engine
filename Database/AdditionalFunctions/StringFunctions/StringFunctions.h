#pragma once
#include <string>
#include <vector>

using namespace std;

namespace StringFunctions {
    static string Concat(const vector<string>& strings);
    static int Ascii(const string& str);
    static string Char(const int& asciiCode);
    static int CharIndex(const string& subStr, const string& str, const int& startIndex = 0);
    static int DataLength(const string& str);
    static string Left(const string& str, const int& numberOfCharacters = 0);
    static string Right(const string& str, const int& numberOfCharacters = 0);
    static int Length(const string& str);
    static string Lower(const string& str);
    static string Upper(const string& str);
    static string Trim(const string& str);
    static string TrimLeft(const string& str);
    static string TrimRight(const string& str);
    static string Replace(const string& str, const string& subStr, const string& replaceStr);
    static string SubString(const string& str, const int& startIndex = 0, const int& endIndex = 0);
}