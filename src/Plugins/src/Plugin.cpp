#include "../include/Plugin.h"

#include "PluginApi.h"

#include <iostream>
#ifdef _WIN32
    #include <windows.h>
    using LibHandle = HMODULE;
#else
#include <dlfcn.h>
using LibHandle = void*;
#endif

namespace External {
  bool Plugin::Load(const char *path) {
    LibHandle handle;
#ifdef _WIN32
    handle = LoadLibraryA(path);
    if (!handle) {
      std::cerr << "[Plugin] Failed to load: " << path << std::endl;
      return false;
    }

    const auto pluginInitFunction = reinterpret_cast<int(*)(const IHostAPI*)>(GetProcAddress(handle, "PluginInit"));
    if (!pluginInitFunction) {
      std::cerr << "[Plugin] PluginInit not found in: " << path << std::endl;
      return false;
    }

#else
    handle = dlopen(path, RTLD_NOW);
    if (!handle) {
      std::cerr << "[Plugin] Failed to load: " << path << " (" << dlerror() << ")" << std::endl;
      return false;
    }

    const auto pluginInitFunction = reinterpret_cast<int(*)(const IHostAPI*)>(dlsym(handle, "PluginInit"));
    char* err = dlerror();
    if (err != nullptr) {
      std::cerr << "[Plugin] PluginInit not found in: " << path << " (" << err << ")" << std::endl;
      return false;
    }
#endif

    int r = pluginInitFunction(&this->host);
    if (r != 0) {
      std::cerr << "[Plugin] PluginInit failed for: " << path << std::endl;
      return false;
    }

    std::cout << "[Plugin] Loaded plugin: " << path << std::endl;
    return true;
  }

  void Plugin::Execute(const std::string &name) {
    FunctionDescriptor descriptor;
    if (!registry.TryGetValue(name, descriptor)) {
      std::cerr << "[Plugin] Function not found: " << name << std::endl;
      return;
    }

    PluginValue args[2];

    args[0] = PluginValue{ .ctx = reinterpret_cast<void*>(10) };
    args[1] = PluginValue{ .ctx =  reinterpret_cast<void*>(10) };

    auto [ctx] = descriptor.fn(nullptr, 2, args);

    std::cout << "[Plugin] Function " << name << " executed. Result ctx: " << reinterpret_cast<uintptr_t>(ctx) << std::endl;
  }

  void Plugin::Log(int level, const char *message){
    std::cout << "[Plugin Log]: " << message << std::endl;
  }

  int Plugin::RegisterScalar(const char *name, const udf_func_t fn, const int min_args, const int max_args, const char *help) {
      const auto strName = std::string(name);

      auto functionDesc = FunctionDescriptor{
          .fn = fn,
          .min_args = min_args,
          .max_args = max_args,
          .help = std::string(help)
      };

      registry.Add(strName, std::move(functionDesc));

      std::cout << "[Engine] Registered scalar function: " << strName << std::endl;
      return 0;
  }

  Plugin::Plugin() {
    this->host = IHostAPI{
        .api_version = 1,
        .log = Plugin::Log,
        .register_scalar_function = Plugin::RegisterScalar
    };
  }

}