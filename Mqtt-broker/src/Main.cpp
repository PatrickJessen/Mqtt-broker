#include "Mqtt/MqttServer.h"
#include "UnitTest.h"

static bool enableUnitTest = true;
int main()
{
    try {
        if (enableUnitTest) {
            UnitTest test;
            test.Run();
        }
        else
        {
            boost::asio::io_service io_service;
            MqttServer mqttServer(io_service, 1883); // MQTT default port

            io_service.run();
        }
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}