#pragma once

namespace Cpu{
    struct Features{
        bool _avx512f;
        bool _avx512bw;
        bool _vpclmulqdq;
    };

    [[nodiscard]] inline Features Features(){
        static const struct Features features{
            ._avx512f = __builtin_cpu_supports("avx512f") != 0,
            ._avx512bw = __builtin_cpu_supports("avx512bw") != 0,
            ._vpclmulqdq = __builtin_cpu_supports("vpclmulqdq") != 0
        };
        return features;
    }
}