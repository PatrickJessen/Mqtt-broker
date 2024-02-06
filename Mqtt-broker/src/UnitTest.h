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
    void parseConnect();
    void parseSubscription();
    void parsePublish();

private:
    std::vector<TestResult> testResults;
};