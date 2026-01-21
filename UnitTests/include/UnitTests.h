#pragma once

namespace Tests{
    using TestFunctionPtr = void (*)();

    void SignalHandler(int signal);
    void InitializeTester();
    void RunTest(TestFunctionPtr functionPtr);

    void IndexPageUpdate();
}
