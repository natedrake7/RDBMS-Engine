#pragma once

#ifdef _WIN32
    #define PLUGIN_EXPORT __declspec(dllexport)
    #define PLUGIN_CALL __stdcall
#else
    #define PLUGIN_EXPORT __attribute__((visibility("default")))
    #define PLUGIN_CALL
#endif

#include <stdint.h>

typedef struct {
  void* ctx;
} PluginValue;

typedef PluginValue (*udf_func_t)(void* thd_ctx, int argc, const PluginValue* argv);

extern "C" {
    PLUGIN_EXPORT void PLUGIN_CALL initialize_plugin();
    PLUGIN_EXPORT void PLUGIN_CALL shutdown_plugin();
    PLUGIN_EXPORT udf_func_t PLUGIN_CALL get_udf_function(const char* name);
}

struct IHostAPI {
  int api_version;
  void (*log)(int level, const char* message);
  int (*register_scalar_function)(const char* name, udf_func_t fn, int min_args, int max_args, const char* help);
};

#ifdef __cplusplus
extern "C" {
#endif

  PLUGIN_EXPORT int PLUGIN_CALL PluginInit(const IHostAPI* host);
  PLUGIN_EXPORT void PLUGIN_CALL PluginShutdown();

#ifdef __cplusplus
}
#endif