#pragma once
#include "BodyProtocol.h"
#include <string>

using namespace std;

typedef struct AuthorizeBody{
  string username;
  string password;

  AuthorizeBody() = default;

  AuthorizeBody(const string& username, const string& password): username(username), password(password) {}
  
  ~AuthorizeBody() = default;
  
  [[nodiscard]] int GetBodySize() const {
    return username.size() + password.size() + 2 * sizeof(int);
  }
}AuthorizeBody;