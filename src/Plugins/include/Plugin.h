#pragma once
#include "PluginApi.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"

#include <string>
#include <unordered_map>

namespace External {
  struct FunctionDescriptor {
    udf_func_t fn;
    int min_args;
    int max_args;
    std::string help;
  };

  class Plugin {
    IHostAPI host;

    static void Log(int level, const char* message);
    static int RegisterScalar(const char* name, udf_func_t fn, int min_args, int max_args, const char* help);

    public:
      Plugin();
      bool Load(const char* path);
      static void Execute(const std::string& name);
  };

    inline Dictionary<std::string, FunctionDescriptor> registry;
}