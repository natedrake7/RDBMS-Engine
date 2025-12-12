#include "../include/PluginManager.h"

#include "PluginApi.h"

#include <iostream>
#ifdef _WIN32
    #include <windows.h>
    using LibHandle = HMODULE;
#else
#include <dlfcn.h>
using LibHandle = void*;
#endif

namespace Plugins {
  bool Manager::LoadPlugin(const char *path) {
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
    handle = dlopen(path.c_str(), RTLD_NOW);
    if (!handle) {
      std::cerr << "[Plugin] Failed to load: " << path << " (" << dlerror() << ")" << std::endl;
      return false;
    }

    auto PluginInitFn = (int(*)(const HostAPI*))dlsym(handle, "PluginInit");
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

  void Manager::Log(int level, const char *message){
    std::cout << "[Plugin Log]: " << message << std::endl;
  }

  Manager::Manager() {
    this->host = IHostAPI{
        .version = 1,
        .log = Manager::Log,
    };
  }

}