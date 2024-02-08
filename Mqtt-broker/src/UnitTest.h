#pragma once
#include "Mqtt/MqttParser.h"

struct TestResult
{
    std::string functionName;
    bool isSuccessfull;
    std::string result;
};

class UnitTest
{
public:
    UnitTest() = default;

public:
    void Run();

private:
    void Test_parseConnect();
    void Test_parseSubscription();
    void Test_parsePublish();

private:
    std::vector<TestResult> testResults;
};