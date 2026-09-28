#pragma once

#if defined(__GNUC__) && !defined(__clang__) && !defined(__INTEL_COMPILER) && !defined(__INTEL_LLVM_COMPILER)
  #define IS_GCC 1
#else
  #define IS_GCC 0
#endif