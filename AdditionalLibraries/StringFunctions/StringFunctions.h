#pragma once
#include <algorithm>
#include <codecvt>
#include <locale>
#include <string>

namespace AdditionalLibraries {
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

  inline std::string Lower(const std::string &str) {
      if(str.empty())
        return str;

      std::string result;

      for(const auto& character : str)
        result += static_cast<char>(tolower(character));

      return result;
  }

  inline std::u16string ToUnicode(const std::string &str) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> convert;

    return convert.from_bytes(str);
  }
}



