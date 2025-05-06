#pragma once
#include <cstring>
#include <string>

using namespace std;

typedef struct AuthorizeBody {
  string username;
  string password;

  AuthorizeBody() = default;

  AuthorizeBody(const string& username, const string& password): username(username), password(password) {}
  
  ~AuthorizeBody() = default;
  
  int GetBodySize() const {
    return username.size() + password.size() + 2 * sizeof(int);
  }
}AuthorizeBody;