#pragma once
#include <algorithm>
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

  inline std::string Lower(const std::string &str) {
      if(str.empty())
        return str;

      std::string result;

      for(const auto& character : str)
        result += static_cast<char>(tolower(character));

      return result;
  }
}



