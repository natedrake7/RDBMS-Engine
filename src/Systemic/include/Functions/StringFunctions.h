#pragma once
#include <algorithm>
#include <string>
#include <vector>
#include "../DataTypes/DataTypes.h"

namespace Functions::String {
  inline std::string NormalizeString(const std::string &str) {
    auto temp = str;
    std::ranges::transform(temp, temp.begin(), ::tolower);
    return temp;
  }

  inline std::string RemoveQuotesFromString(const std::string &str) {
    return str.substr(1, str.size() - 2);
  }

  inline std::string RemoveQuotesFromUnicodeString(const std::string &str) {
    return str.substr(2, str.size() - 3);
  }

  inline std::string Space(const int& size) {
      return std::string(size, ' ');
  }
  //
  // inline std::string Lower(const std::string &str) {
  //     if(str.empty())
  //       return str;
  //
  //     std::string result;
  //
  //     for(const auto& character : str)
  //       result += static_cast<char>(tolower(character));
  //
  //     return result;
  // }

    inline std::string Concat(const std::vector<std::string>& strings)
    {
        std::string result;
        for(const auto& string : strings)
            result.append(string);

        return result;
    }

    inline int Ascii(const std::string &str)
    {
        if(str.empty())
            return 0;

        return str.front();
    }

    inline std::string Char(const Int asciiCode) { return {0, static_cast<char>(asciiCode)}; }

    inline int CharIndex(const std::string &subStr, const std::string &str, const Int startIndex)
    {
        if(subStr.empty()
            || str.empty()
            || startIndex < 0
            || startIndex > str.size()
            || subStr.size() > str.size())
            return 0;

        return static_cast<int>(str.find(subStr, startIndex));
    }

    inline int DataLength(const std::string &str) { return static_cast<int>(str.size()); }

    inline std::string Left(const std::string &str, const Int numberOfCharacters)
    {
        if(str.empty() || numberOfCharacters <= 0)
            return str;

        return str.substr(0, numberOfCharacters);
    }

    inline std::string Right(const std::string &str, const Int numberOfCharacters)
    {
        if(str.empty() || numberOfCharacters <= 0)
            return str;

        return str.substr(str.size() - numberOfCharacters, numberOfCharacters);
    }

    inline std::string Lower(const std::string &str)
    {
        if(str.empty())
            return str;

        std::string result;

        for(const auto& character : str)
            result += static_cast<char>(tolower(character));

        return result;
    }

    inline std::string Upper(const std::string &str)
    {
        if(str.empty())
            return str;

        std::string result;

        for(const auto& character : str)
            result += static_cast<char>(toupper(character));

        return result;
    }

    inline std::string Trim(const std::string &str)
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

    inline int Length(const std::string &str)
    {
      if(str.empty())
          return 0;

      return static_cast<int>(str.size());
    }

    inline std::string TrimLeft(const std::string &str)
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

    inline std::string TrimRight(const std::string &str)
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

        return str.substr(0, lastIndex + 1);
    }

    inline std::string Replace(const std::string &str, const std::string &subStr, const std::string &replaceStr)
    {
        if(str.empty() || subStr.empty())
            return str;

        std::string result(str);
        int subStrIndex = 0;

        while (true)
        {
            subStrIndex = static_cast<int>(result.find(subStr));

            if(subStrIndex == std::string::npos)
                break;

            result.replace(subStrIndex, subStr.size(), replaceStr);
        }

        return result;
    }

    inline std::string SubString(const std::string &str, const Int startIndex, const Int endIndex)
    {
        if(startIndex > str.size() -1 || endIndex > str.size() - 1)
            return str;

        return str.substr(startIndex, endIndex - startIndex + 1);
    }

    inline std::string Reverse (const std::string &str) {
      return {str.rbegin(), str.rend()};
    }

    inline bool EqualsIgnoreCase(const std::string& a, const std::string& b)
    {
      if (a.size() != b.size())
          return false;

      for (int i = 0; i < a.size(); i++) {
          if (std::tolower(a[i]) != std::tolower(b[i]))
              return false;
      }

      return true;
    }

    inline int CompareIgnoreCase(const std::string& a, const std::string& b)
    {
      const auto n = std::min(a.size(), b.size());

      for (int i = 0; i < n; i++) {
        const int la = std::tolower(a[i]);
        const int lb = std::tolower(b[i]);

        if (la < lb) return -1;
        if (la > lb) return 1;
      }

      if (a.size() < b.size()) return -1;
      if (a.size() > b.size()) return 1;

      return 0;
    }

    inline bool StartsWithIgnoreCase(const std::string& s, const std::string& prefix)
    {
      if (prefix.size() > s.size()) return false;

      for (int i = 0; i < prefix.size(); i++)
        if (std::tolower(s[i]) != std::tolower(prefix[i])) return false;

      return true;
    }

    inline bool EndsWithIgnoreCase(const std::string& s, const std::string& suffix)
    {
      if (suffix.size() > s.size()) return false;

      const auto offset = s.size() - suffix.size();

      for (int i = 0; i < suffix.size(); i++)
        if (std::tolower(s[offset + i]) != std::tolower(suffix[i])) return false;

      return true;
    }
}



