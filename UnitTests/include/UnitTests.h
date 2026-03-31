#pragma once

namespace Tests{
    using TestFunctionPtr = void (*)();

    void SignalHandler(int signal);
    void InitializeTester();
    void RunTest(TestFunctionPtr functionPtr);

    void DecimalTest();
    void IndexPageUpdate();
    void PageUpdate();
}
