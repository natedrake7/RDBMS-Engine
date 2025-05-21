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
}



