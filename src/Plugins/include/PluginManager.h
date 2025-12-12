#pragma once
#include "PluginApi.h"

namespace Plugins {
  class Manager {
    IHostAPI host;

    static void Log(int level, const char* message);

    public:
      Manager();
      bool LoadPlugin(const char* path);
  };
}