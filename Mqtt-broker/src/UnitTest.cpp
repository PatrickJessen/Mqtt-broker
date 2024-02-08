#include "UnitTest.h"

static const std::string& boolToString_(bool val)
{
    if (val) {
        return "true";
    }
    return "false";
}

void UnitTest::Run()
{
    Test_parseConnect();
    Test_parseSubscription();
    Test_parsePublish();

    std::cout << "Test results\n";
    for (int i = 0; i < testResults.size(); i++) {
        if (!testResults[i].isSuccessfull) {
            std::cout << "Function: " << testResults[i].functionName << " Is Successfull: " << testResults[i].isSuccessfull 
                << " Expected result:" << testResults[i].result << "\n";
        }
        else {
            std::cout << "Function: " << testResults[i].functionName << " Is Successfull: " << testResults[i].isSuccessfull << "\n";
        }
    }
}

void UnitTest::Test_parseConnect()
{
    // Connect packet data
    std::vector<unsigned char> hexVal = {
        0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xC6, 0x00, 0x3C, 0x00, 0x00, 0x00, 0x0A, 0x77, 0x69,
        0x6C, 0x6C, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x00, 0x0C, 0x57, 0x69, 0x6C, 0x6C, 0x20, 0x6D,
        0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x00, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6E, 0x61, 0x6D, 0x65,
        0x00, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64
    };
    std::vector<char> connectData(hexVal.begin(), hexVal.end());

    // Parse the data
    MqttParser connect(connectData);
    ConnectOptions connectOptions = connect.parseConnect();

    // Expected result
    std::string expectedUsername = "username";
    std::string expectedPassword = "password";
    if (connectOptions.username == expectedUsername && connectOptions.password == expectedPassword) {
        testResults.push_back({ "parseConnect()", true });
        return;
    }
    std::string expected = "Expected ";
    expected.append(expectedUsername + " but got " + connectOptions.username + ". Expected " +
        expectedPassword + " but got " + connectOptions.password);
    testResults.push_back({ "parseConnect()", false, expected});
}

void UnitTest::Test_parseSubscription()
{
    // Subscription packet data
    std::vector<unsigned char> hexVal = {
        0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00
    };
    std::vector<char> subscription(hexVal.begin(), hexVal.end());

    // Parse the data
    MqttParser sub(subscription);
    SubscriptionOptions subscriptionOptions = sub.parseSubscription();

    // Expected result
    QoSType expectedQoS = QoSType::QOS0;
    std::string expectedTopic = "test";
    if (subscriptionOptions.qosLevel == expectedQoS && subscriptionOptions.topic == expectedTopic) {
        testResults.push_back({ "parseSubscription()", true });
        return;
    }
    std::string expected = "Expected ";
    expected.append(std::to_string((int)expectedQoS) + " but got " + std::to_string((int)subscriptionOptions.qosLevel) +
        ". Expected " + expectedTopic + " but got " + subscriptionOptions.topic);
    testResults.push_back({ "parseSubscription()", false, expected });
}

void UnitTest::Test_parsePublish()
{
    std::vector<unsigned char> hexVal = {
        0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21
    };
    std::vector<char> publish(hexVal.begin(), hexVal.end());
    MqttParser pub(publish);
    PublishPacket publishOptions = pub.parsePublish();
    std::string expectedPayload = "Hello world!";
    std::string expectedTopic = "test";
    if (publishOptions.payload == expectedPayload && publishOptions.topic == expectedTopic) {
        testResults.push_back({ "parsePublish()", true });
        return;
    }
    std::string expected = "Expected ";
    expected.append(expectedPayload);
    expected.append(" but got ");
    expected.append(publishOptions.payload);
    expected.append(". Expected ");
    expected.append(expectedTopic);
    expected.append(" but got ");
    expected.append(publishOptions.topic);
    testResults.push_back({ "parsePublish()", false, expected });
}
