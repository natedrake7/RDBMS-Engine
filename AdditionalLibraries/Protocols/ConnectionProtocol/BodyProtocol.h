#pragma once
#include <string>

using namespace std;

typedef struct Body {
  int code;
  string reason;

  Body(): code(0) {}
  Body(const int& code, const string& reason): code(code), reason(reason) {}
  virtual ~Body() = default;

  [[nodiscard]] virtual int GetBodySize() const { return reason.size() + sizeof(int);}
}Body;