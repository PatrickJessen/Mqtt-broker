#include "Mqtt/Mqtt_Broker.h"


int main()
{
    try {
        io_service io_service;
        MqttBroker mqttBroker(io_service, 1883); // MQTT default port

        io_service.run();
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}